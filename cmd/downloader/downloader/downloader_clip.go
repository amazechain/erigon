package downloader

import (
	"context"
	"fmt"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	downloader2 "github.com/ledgerwatch/erigon-lib/downloader"
	"github.com/ledgerwatch/erigon-lib/downloader/downloadercfg"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/turbo/clipdb"
	"github.com/ledgerwatch/log/v3"
	mdbx2 "github.com/torquem-ch/mdbx-go/mdbx"
	"golang.org/x/sync/semaphore"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type ClipDownloader struct {
	db                kv.RwDB
	pieceCompletionDB storage.PieceCompletion
	torrentClient     *torrent.Client
	clientLock        *sync.RWMutex

	cfg *downloadercfg.Cfg

	statsLock *sync.RWMutex
	stats     downloader2.AggStats

	folder storage.ClientImplCloser
}

func Clip(cfg *downloadercfg.Cfg, t clipdb.Torrent) (*ClipDownloader, error) {
	if err := portMustBeTCPAndUDPOpen(cfg.ListenPort); err != nil {
		return nil, err
	}

	db, c, m, torrentClient, err := openClient(cfg.ClientConfig)
	if err != nil {
		return nil, fmt.Errorf("openClient: %w", err)
	}

	peerID, err := readPeerID(db)
	if err != nil {
		return nil, fmt.Errorf("get peer id: %w", err)
	}
	cfg.PeerID = string(peerID)
	if len(peerID) == 0 {
		if err = savePeerID(db, torrentClient.PeerID()); err != nil {
			return nil, fmt.Errorf("save peer id: %w", err)
		}
	}

	d := &ClipDownloader{
		cfg:               cfg,
		db:                db,
		pieceCompletionDB: c,
		folder:            m,
		torrentClient:     torrentClient,
		clientLock:        &sync.RWMutex{},

		statsLock: &sync.RWMutex{},
	}
	torrentFileName := filepath.Join(cfg.DataDir, t.Filename())
	var metaInfo *metainfo.MetaInfo

	if !dir.FileExist(torrentFileName) {
		response, err := http.Get(t.URL())
		if err != nil {
			return nil, fmt.Errorf("Error downloading torrent file: %s", err)
		}
		defer response.Body.Close()

		metaInfo, err = metainfo.Load(response.Body)
		if err != nil {
			return nil, fmt.Errorf("error loading torrent file from %q: %s\n", t.URL(), err)
		}
	} else {
		metaInfo, err = metainfo.LoadFromFile(torrentFileName)
		if err != nil {
			return nil, fmt.Errorf("error loading torrent file from %q: %s\n", torrentFileName, err)
		}
	}

	file, err := os.Create(torrentFileName)
	if err != nil {
		return nil, err
	}
	defer file.Sync()
	defer file.Close()
	if err := metaInfo.Write(file); err != nil {
		return nil, err
	}

	d.torrentClient.AddTorrent(metaInfo)
	return d, nil
}

func (d *ClipDownloader) SnapDir() string {
	d.clientLock.RLock()
	defer d.clientLock.RUnlock()
	return d.cfg.DataDir
}

func (d *ClipDownloader) ReCalcStats(interval time.Duration) {
	//Call this methods outside of `statsLock` critical section, because they have own locks with contention
	torrents := d.torrentClient.Torrents()
	connStats := d.torrentClient.ConnStats()
	peers := make(map[torrent.PeerID]struct{}, 16)

	d.statsLock.Lock()
	defer d.statsLock.Unlock()
	prevStats, stats := d.stats, d.stats

	stats.Completed = true
	stats.BytesDownload = uint64(connStats.BytesReadUsefulIntendedData.Int64())
	stats.BytesUpload = uint64(connStats.BytesWrittenData.Int64())

	stats.BytesTotal, stats.BytesCompleted, stats.ConnectionsTotal, stats.MetadataReady = 0, 0, 0, 0
	for _, t := range torrents {
		select {
		case <-t.GotInfo():
			stats.MetadataReady++
			for _, peer := range t.PeerConns() {
				stats.ConnectionsTotal++
				peers[peer.PeerID] = struct{}{}
			}
			stats.BytesCompleted += uint64(t.BytesCompleted())
			stats.BytesTotal += uint64(t.Length())
		default:
		}

		stats.Completed = stats.Completed && t.Complete.Bool()
	}

	stats.DownloadRate = (stats.BytesDownload - prevStats.BytesDownload) / uint64(interval.Seconds())
	stats.UploadRate = (stats.BytesUpload - prevStats.BytesUpload) / uint64(interval.Seconds())

	if stats.BytesTotal == 0 {
		stats.Progress = 0
	} else {
		stats.Progress = float32(float64(100) * (float64(stats.BytesCompleted) / float64(stats.BytesTotal)))
		if stats.Progress == 100 && !stats.Completed {
			stats.Progress = 99.99
		}
	}
	stats.PeersUnique = int32(len(peers))
	stats.FilesTotal = int32(len(torrents))

	d.stats = stats
}

func (d *ClipDownloader) Stats() downloader2.AggStats {
	d.statsLock.RLock()
	defer d.statsLock.RUnlock()
	return d.stats
}

func (d *ClipDownloader) Close() {
	d.torrentClient.Close()
	if err := d.folder.Close(); err != nil {
		log.Warn("[Snapshots] folder.close", "err", err)
	}
	if err := d.pieceCompletionDB.Close(); err != nil {
		log.Warn("[Snapshots] pieceCompletionDB.close", "err", err)
	}
	d.db.Close()
}

func (d *ClipDownloader) PeerID() []byte {
	peerID := d.torrentClient.PeerID()
	return peerID[:]
}

func (d *ClipDownloader) StopSeeding(hash metainfo.Hash) error {
	t, ok := d.torrentClient.Torrent(hash)
	if !ok {
		return nil
	}
	ch := t.Closed()
	t.Drop()
	<-ch
	return nil
}

func (d *ClipDownloader) Torrent() *torrent.Client {
	d.clientLock.RLock()
	defer d.clientLock.RUnlock()
	return d.torrentClient
}

func ClipLoop(ctx context.Context, d *ClipDownloader, silent bool) {
	var sem = semaphore.NewWeighted(int64(d.cfg.DownloadSlots))

	go func() {
		for {
			torrents := d.Torrent().Torrents()
			if len(torrents) <= 0 {
				return
			}
			for _, t := range torrents {
				<-t.GotInfo()
				if t.Complete.Bool() {
					t.Drop()
					continue
				}
				if err := sem.Acquire(ctx, 1); err != nil {
					return
				}
				t.AllowDataDownload()
				t.DownloadAll()
				go func(t *torrent.Torrent) {
					defer sem.Release(1)
					//r := t.NewReader()
					//r.SetReadahead(t.Length())
					//_, _ = io.Copy(io.Discard, r) // enable streaming - it will prioritize sequential download

					<-t.Complete.On()
				}(t)
			}
			time.Sleep(30 * time.Second)
		}
	}()

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	statInterval := 20 * time.Second
	statEvery := time.NewTicker(statInterval)
	defer statEvery.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-statEvery.C:
			d.ReCalcStats(statInterval)

		case <-logEvery.C:
			if silent {
				continue
			}

			stats := d.Stats()

			if stats.MetadataReady < stats.FilesTotal {
				log.Info(fmt.Sprintf("[ClipDB] Waiting for torrents metadata: %d/%d", stats.MetadataReady, stats.FilesTotal))
				continue
			}

			if stats.Completed {
				return
			}

			log.Info("[ClipDB] Downloading",
				"progress", fmt.Sprintf("%.2f%% %s/%s", stats.Progress, common2.ByteCount(stats.BytesCompleted), common2.ByteCount(stats.BytesTotal)),
				"download", common2.ByteCount(stats.DownloadRate)+"/s",
				"upload", common2.ByteCount(stats.UploadRate)+"/s",
				"peers", stats.PeersUnique,
				"connections", stats.ConnectionsTotal,
				"files", stats.FilesTotal)
			if stats.PeersUnique == 0 {
				ips := d.Torrent().BadPeerIPs()
				if len(ips) > 0 {
					log.Info("[ClipDB] Stats", "banned", ips)
				}
			}
		}
	}
}

func openClient(cfg *torrent.ClientConfig) (db kv.RwDB, c storage.PieceCompletion, m storage.ClientImplCloser, torrentClient *torrent.Client, err error) {
	snapDir := cfg.DataDir
	db, err = mdbx.NewMDBX(log.New()).
		Flags(func(f uint) uint { return f | mdbx2.SafeNoSync }).
		Label(kv.DownloaderDB).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.DownloaderTablesCfg }).
		SyncPeriod(15 * time.Second).
		Path(filepath.Join(snapDir, "db")).
		Open()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	c, err = downloader2.NewMdbxPieceCompletion(db)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("torrentcfg.NewMdbxPieceCompletion: %w", err)
	}
	m = storage.NewMMapWithCompletion(snapDir, c)
	cfg.DefaultStorage = m

	for retry := 0; retry < 5; retry++ {
		torrentClient, err = torrent.NewClient(cfg)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("torrent.NewClient: %w", err)
	}

	return db, c, m, torrentClient, nil
}
