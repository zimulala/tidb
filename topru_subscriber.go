package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pingcap/tipb/go-tipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:10080", "TiDB status gRPC address")
	interval := flag.String("interval", "15s", "TopRU report interval: 15s|30s|60s")
	flag.Parse()

	reportInterval := parseInterval(*interval)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	dialCtx, dialCancel := context.WithTimeout(ctx, 5*time.Second)
	defer dialCancel()

	conn, err := grpc.DialContext(
		dialCtx,
		*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Fatalf("dial %s failed: %v", *addr, err)
	}
	defer conn.Close()

	client := tipb.NewTopSQLPubSubClient(conn)
	stream, err := client.Subscribe(ctx, &tipb.TopSQLSubRequest{
		Collectors: []tipb.CollectorType{tipb.CollectorType_COLLECTOR_TYPE_TOPRU},
		Topru: &tipb.TopRUConfig{
			ReportIntervalSeconds: reportInterval,
		},
	})
	if err != nil {
		log.Fatalf("subscribe failed: %v", err)
	}

	log.Printf("subscribed to %s, enable_top_ru=true, interval=%s", *addr, *interval)

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			log.Println("stream closed by server")
			return
		}
		if err != nil {
			log.Fatalf("recv failed: %v", err)
		}

		switch {
		case resp.GetRuRecord() != nil:
			r := resp.GetRuRecord()
			fmt.Printf("[RU] user=%s sql_digest=%x plan_digest=%x items=%d\n",
				r.GetUser(), r.GetSqlDigest(), r.GetPlanDigest(), len(r.GetItems()))
		case resp.GetRecord() != nil:
			rec := resp.GetRecord()
			fmt.Printf("[TopSQL] sql_digest=%x items=%d\n", rec.GetSqlDigest(), len(rec.GetItems()))
		case resp.GetSqlMeta() != nil:
			m := resp.GetSqlMeta()
			fmt.Printf("[SQLMeta] digest=%x internal=%v\n", m.GetSqlDigest(), m.GetIsInternalSql())
		case resp.GetPlanMeta() != nil:
			m := resp.GetPlanMeta()
			fmt.Printf("[PlanMeta] digest=%x plan_len=%d\n", m.GetPlanDigest(), len(m.GetNormalizedPlan()))
		default:
			fmt.Println("[Unknown] empty payload")
		}
	}
}

func parseInterval(s string) tipb.ReportInterval {
	switch s {
	case "15s":
		return tipb.ReportInterval_REPORT_INTERVAL_15S
	case "30s":
		return tipb.ReportInterval_REPORT_INTERVAL_30S
	case "60s":
		return tipb.ReportInterval_REPORT_INTERVAL_60S
	default:
		log.Printf("unknown interval %q, fallback to 15s", s)
		return tipb.ReportInterval_REPORT_INTERVAL_15S
	}
}
