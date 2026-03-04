package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	ads "github.com/RuneRoven/go-ads"
)

func main() {
	ip := flag.String("ip", "192.168.3.224", "PLC IP address")
	netid := flag.String("netid", "5.154.236.19.1.1", "Target AMS NetID")
	port := flag.Int("port", 48898, "TCP port")
	amsport := flag.Int("amsport", 851, "Target AMS port")
	localnetid := flag.String("localnetid", "auto", "Local AMS NetID (auto = derive from local IP)")
	localport := flag.Int("localport", 10500, "Local AMS port")
	symbol := flag.String("symbol", "", "Symbol name to read")
	list := flag.Bool("list", false, "List all discovered symbols")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	conn, err := ads.NewConnection(ctx, *ip, *port, *netid, *amsport, *localnetid, *localport, 5*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating connection: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Connecting to %s:%d (AMS %s:%d)...\n", *ip, *port, *netid, *amsport)
	err = conn.Connect(false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	fmt.Println("Connected.")

	// Device info
	info, err := conn.ReadDeviceInfo()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading device info: %v\n", err)
	} else {
		name := strings.TrimRight(string(info.DeviceName[:]), "\x00")
		fmt.Printf("Device: %s  Version: %d.%d.%d\n", name, info.Major, info.Minor, info.Version)
	}

	// Device state
	state, err := conn.ReadState()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading state: %v\n", err)
	} else {
		fmt.Printf("ADS State: %d  Device State: %d\n", state.AdsState, state.DeviceState)
	}

	// List symbols
	if *list {
		symbols := conn.ListSymbols()
		fmt.Printf("\nSymbols (%d):\n", len(symbols))
		for name, sym := range symbols {
			fmt.Printf("  %-50s  type=%-20s  size=%d\n", name, sym.DataType, sym.Length)
		}
	}

	// Read a specific symbol
	if *symbol != "" {
		value, err := conn.ReadFromSymbol(*symbol)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading symbol %q: %v\n", *symbol, err)
			os.Exit(1)
		}
		fmt.Printf("\n%s = %s\n", *symbol, value)
	}
}
