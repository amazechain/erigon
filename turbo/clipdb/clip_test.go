package clipdb

import (
	"context"
	"reflect"
	"testing"
)

func TestUncompress(t *testing.T) {
	type args struct {
		ctx  context.Context
		from string
		to   string
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "test",
			args: args{
				ctx:  context.Background(),
				from: "/Users/mac/devel/snap/snapshots/chaindata7z.7z",
				to:   "/Users/mac/devel/snap",
			},
			want: []string{
				"mdbx.dat",
				"mdbx.lck",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Uncompress(tt.args.ctx, tt.args.from, tt.args.to)
			if (err != nil) != tt.wantErr {
				t.Errorf("Uncompress() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Uncompress() got = %v, want %v", got, tt.want)
			}
		})
	}
}
