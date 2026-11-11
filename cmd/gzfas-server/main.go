// Command gzfas-server runs GZFAS with its dashboard over HTTP.
//
// With a real ZooKeeper:
//
//	gzfas-server -zk localhost:2181
//
// Without ZooKeeper, on the in-memory store:
//
//	gzfas-server -memory
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rahelmartim/gzfas/coord"
	"github.com/rahelmartim/gzfas/dashboard"
	"github.com/rahelmartim/gzfas/gzfas"
	"github.com/rahelmartim/gzfas/web"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "HTTP address of the dashboard")
	zk := flag.String("zk", "localhost:2181", "comma-separated ZooKeeper servers")
	memory := flag.Bool("memory", false, "use the in-memory store instead of ZooKeeper")
	servers := flag.String("servers", "s1,s2,s3", "GZFAS servers to start in this process")
	dataDir := flag.String("data", "gzfas-data", "folder for the file content (empty: in memory)")
	flag.Parse()

	var store coord.Store = &coord.ZooKeeperStore{Servers: strings.Split(*zk, ","), Timeout: 10 * time.Second}
	mode := "zookeeper " + *zk
	if *memory {
		store, mode = coord.NewMemoryStore(), "memory"
	}
	var blobs gzfas.BlobStore
	if *dataDir != "" {
		blobs = gzfas.DirBlobs{Dir: *dataDir}
	}
	engine, err := dashboard.New(dashboard.Options{
		Store: store, Blobs: blobs, Config: gzfas.Config{MaxConn: 8, MaxRead: 3},
		Servers: splitNonEmpty(*servers),
	})
	if err != nil {
		log.Fatalf("starting GZFAS on %s: %v", mode, err)
	}
	defer engine.Close()

	server := &http.Server{
		Addr:              *addr,
		Handler:           dashboard.Handler(engine, mode, web.Files),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		log.Printf("GZFAS dashboard on http://%s (store: %s)", *addr, mode)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	fmt.Println()
	log.Print("stopping: the sessions end, so ZooKeeper removes the ephemeral znodes")
	_ = server.Close()
}

func splitNonEmpty(list string) []string {
	var out []string
	for _, part := range strings.Split(list, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
