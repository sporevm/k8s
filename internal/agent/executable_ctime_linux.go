package agent

import (
	"os"
	"syscall"
	"time"
)

// changeTime returns the inode change time from info.
func changeTime(info os.FileInfo) time.Time {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return time.Time{}
	}
	return time.Unix(stat.Ctim.Unix())
}
