package clipdb

import (
	"bufio"
	dbTorrent "github.com/amazeDevs/torrents"
	"strings"
)

const downloadURL = "https://raw.githubusercontent.com/amazeDevs/torrents/main"

var dbReleases = split(dbTorrent.LatestTorrent)

func split(txt string) (lines []string) {
	sc := bufio.NewScanner(strings.NewReader(txt))
	for sc.Scan() {
		l := sc.Text()
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		lines = append(lines, sc.Text())
	}

	if err := sc.Err(); err != nil {
		panic(err)
	}
	return lines
}

func First(n int, in []string) (res []string) {
	if n <= len(in) {
		return in[:n]
	}
	return in
}

type Torrent struct {
	prefix   string
	fileName string
}

func LatestTorrent() Torrent {
	return Torrent{
		prefix:   downloadURL,
		fileName: latestTorrentFile(),
	}
}

func (t Torrent) URL() string {
	return t.prefix + "/" + t.fileName
}

func (t Torrent) Filename() string {
	return t.fileName
}

func latestTorrentFile() string {
	return First(1, dbReleases)[0]
}
