package main

import (
	"context"
	"fmt"
	"github.com/jnschaeffer/gosepta/db"
	"github.com/jnschaeffer/gosepta/transitview"
	"go.uber.org/zap"
	"log"
	"time"
	"flag"
	"os"
)

func printVehiclePosition(p transitview.VehiclePosition) {
	fmt.Printf(
		"%s (#%s): %s towards %s\n",
		p.Label,
		p.VehicleID,
		p.Direction,
		p.Destination,
	)
	fmt.Printf(
		"offset: %02d:%02d\n",
		p.OffsetMinutes,
		p.OffsetSeconds,
	)
	fmt.Printf(
		"late: %d\n",
		p.LateMinutes,
	)
	fmt.Printf(
		"position: %.5f lat, %.5f lon, %d deg\n",
		p.Latitude,
		p.Longitude,
		p.Heading,
	)
	fmt.Println()
}

func insert(ctx context.Context, tvClient *transitview.Client, dbClient *db.Client) error {
	now := time.Now()

	allPositions, errPositions := tvClient.AllVehiclePositions(ctx)
	if errPositions != nil {
		return errPositions
	}

	errInsert := dbClient.InsertVehiclePositions(ctx, now, allPositions)
	if errInsert != nil {
		return errInsert
	}

	return nil
}

func main() {
	var (
		dbURL string
		intervalSeconds int
	)

	flag.StringVar(&dbURL, "db_url", "", "URL for sqlite3 database")
	flag.IntVar(&intervalSeconds, "interval", 60, "polling interval in seconds")

	flag.Parse()

	if dbURL == "" || intervalSeconds <= 0 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	interval := time.Second * time.Duration(intervalSeconds)

	logger, errLogger := zap.NewProduction()
	if errLogger != nil {
		log.Fatalf("error initializing logger: %s", errLogger.Error())
	}

	tvClient := transitview.NewClient()
	dbClient, errClient := db.NewClient(dbURL)
	if errClient != nil {
		log.Fatal(errClient)
	}

	defer dbClient.Close()

	ctx := context.Background()

	errInitialize := dbClient.Initialize(ctx)
	if errInitialize != nil {
		logger.Fatal(errInitialize.Error())
	}

	for {
		errInsert := insert(ctx, tvClient, dbClient)
		if errInsert != nil {
			logger.Error(errInsert.Error())
		} else {
			logger.Info("inserted positions")
		}

		time.Sleep(interval)
	}
}
