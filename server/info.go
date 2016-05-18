package server

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"
)

func infoCommand(c *client) {
	if len(c.args) != 1 && len(c.args) != 2 {
		c.replyAritryError()
		return
	}

	allSections := []string{
		"Server", "Clients", "Memory", "Persistence", "Stats",
		"Replication", "CPU", "Commandstats", "Cluster", "Keyspace",
	}
	defaultSections := []string{
		"Server", "Clients", "Memory", "Persistence", "Stats",
		"Replication", "CPU", "Cluster", "Keyspace",
	}
	sections := defaultSections
	if len(c.args) == 2 {
		arg := strings.ToLower(c.args[1])
		switch arg {
		default:
			sections = nil
			for _, section := range allSections {
				if strings.ToLower(section) == arg {
					sections = []string{section}
					break
				}
			}
		case "all":
			sections = allSections
		case "default":
			sections = defaultSections
		}
	}
	wr := &bytes.Buffer{}
	for i, section := range sections {
		if i > 0 {
			wr.WriteString("\n")
		}
		wr.WriteString("# " + section + "\n")
		switch strings.ToLower(section) {
		case "server":
			writeInfoServer(c, wr)
		case "clients":
			writeInfoClients(c, wr)
		case "memory":
			writeInfoMemory(c, wr)
		case "persistence":
			writeInfoPersistence(c, wr)
		case "stats":
			writeInfoStats(c, wr)
		case "replication":
			writeInfoReplication(c, wr)
		case "cpu":
			writeInfoCPU(c, wr)
		case "commandstats":
			writeInfoCommandStats(c, wr)
		case "cluster":
			writeInfoCluster(c, wr)
		case "Keyspace":
			writeInfoKeyspace(c, wr)
		}
	}
	c.replyBulk(wr.String())
}

var osOnce sync.Once
var osName string

const ptrSize = 32 << (uint64(^uintptr(0)) >> 63)

func writeInfoServer(c *client, w io.Writer) {
	now := time.Now()
	fmt.Fprintf(w, "redis_version:%s\n", c.s.options.Version)
	fmt.Fprintf(w, "redis_mode:%s\n", c.s.mode)
	osOnce.Do(func() {
		osb, err := exec.Command("uname", "-smr").Output()
		if err != nil {
			osName = runtime.GOOS
		} else {
			osName = strings.TrimSpace(string(osb))
		}
	})
	fmt.Fprintf(w, "os:%s\n", osName)
	fmt.Fprintf(w, "arch_bits:%d\n", ptrSize)
	fmt.Fprintf(w, "go_version:%s\n", runtime.Version()[2:])
	fmt.Fprintf(w, "process_id:%d\n", os.Getpid())
	fmt.Fprintf(w, "tcp_port:%s\n", c.s.l.Addr().String()[strings.LastIndex(c.s.l.Addr().String(), ":")+1:])
	fmt.Fprintf(w, "uptime_in_seconds:%d\n", now.Sub(c.s.started)/time.Second)
	fmt.Fprintf(w, "uptime_in_days:%d\n", now.Sub(c.s.started)/time.Hour/24)
	fmt.Fprintf(w, "executable:%s\n", c.s.executable)
}

func human(m uint64) string {
	f := float64(m)
	if f < 1024 {
		return fmt.Sprintf("%.2fB", f)
	} else if f < 1024*1024 {
		return fmt.Sprintf("%.2fK", f/1024)
	} else if f < 1024*1024*1024 {
		return fmt.Sprintf("%.2fM", f/1024/1024)
	}
	return fmt.Sprintf("%.2fG", f/1024/1024/1024)
}

func writeInfoMemory(c *client, w io.Writer) {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Fprintf(w, "used_memory:%d\n", m.Alloc)
	fmt.Fprintf(w, "used_memory_human:%s\n", human(m.Alloc))
	// total_system_memory:17179869184
	// total_system_memory_human:16.00G
}
func writeInfoPersistence(c *client, w io.Writer) {
	// aof_enabled:0
	// aof_rewrite_in_progress:0
	// aof_rewrite_scheduled:0
	// aof_last_rewrite_time_sec:-1
	// aof_current_rewrite_time_sec:-1
	// aof_last_bgrewrite_status:ok
	// aof_last_write_status:ok
}

func writeInfoStats(c *client, w io.Writer) {}
func writeInfoReplication(c *client, w io.Writer) {
	// role:master
	// connected_slaves:0
	// master_repl_offset:0
	// repl_backlog_active:0
	// repl_backlog_size:1048576
	// repl_backlog_first_byte_offset:0
	// repl_backlog_histlen:0
}
func writeInfoCPU(c *client, w io.Writer)          {}
func writeInfoCommandStats(c *client, w io.Writer) {}
func writeInfoCluster(c *client, w io.Writer)      {}
func writeInfoKeyspace(c *client, w io.Writer)     {}

func writeInfoClients(c *client, w io.Writer) {
	fmt.Fprintf(w, "connected_clients:%d\n", len(c.s.clients))
}
