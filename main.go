package main

import (
	"bytes"
	"fmt"
	"github.com/codahale/hdrhistogram"
	"github.com/orbs-network/orbs-network-go/instrumentation/metric"
	"github.com/orbs-network/orbs-network-go/services/blockstorage/adapter/filesystem"
	"github.com/orbs-network/orbs-spec/types/go/primitives"
	"github.com/orbs-network/orbs-spec/types/go/protocol"
	"github.com/orbs-network/scribe/log"
	"io/ioutil"
	"os"
	"time"
)

type localConfig struct {
	dir         string
	chainId     primitives.VirtualChainId
	networkType protocol.SignerNetworkType
}

func (l *localConfig) BlockStorageFileSystemDataDir() string {
	return l.dir
}

func (l *localConfig) BlockStorageFileSystemMaxBlockSizeInBytes() uint32 {
	return 64 * 1024 * 1024
}

func (l *localConfig) VirtualChainId() primitives.VirtualChainId {
	return l.chainId
}

func (l *localConfig) NetworkType() protocol.SignerNetworkType {
	return l.networkType
}

type row struct {
	timestamp                  primitives.TimestampNano
	blockHeight                primitives.BlockHeight
	txCount                    uint64
	medianBlockClosingTimeInMs uint64
}

func main() {
	logger := log.GetLogger().WithOutput(log.NewFormattingOutput(os.Stdout, log.NewHumanReadableFormatter()))
	metricFactory := metric.NewRegistry()

	start := time.Now()

	persistence, err := filesystem.NewBlockPersistence(&localConfig{
		chainId:     1100000,
		networkType: protocol.NETWORK_TYPE_RESERVED,
		dir:         "/Users/kirill/Downloads",
	}, logger, metricFactory)

	logger.Info("startup time", log.String("duration", time.Since(start).String()))

	if err != nil {
		logger.Error("failed to start processing", log.Error(err))
		os.Exit(1)
	}

	lastBlockHeight, err := persistence.GetLastBlockHeight()
	if err != nil {
		logger.Error("failed to start processing", log.Error(err))
		os.Exit(1)
	}

	var rows []row

	var previousBlock *protocol.BlockPairContainer
	if err := persistence.ScanBlocks(1, 1000, func(first primitives.BlockHeight, page []*protocol.BlockPairContainer) (wantsMore bool) {
		totalTxCount := uint64(0)

		blockClosingTime := hdrhistogram.NewWindowed(1, 0, 24*time.Hour.Nanoseconds(), 1)

		for _, block := range page {
			txs := block.TransactionsBlock.SignedTransactions
			totalTxCount += uint64(len(txs))

			if previousBlock != nil {
				previousTimestamp := previousBlock.TransactionsBlock.Header.Timestamp()
				blockClosingTime.Current.RecordValue(int64(block.TransactionsBlock.Header.Timestamp()-previousTimestamp) / 1000000)
			}

			previousBlock = block
		}

		lastBatchBlock := page[len(page)-1]
		lastBatchBlockHeight := lastBatchBlock.TransactionsBlock.Header.BlockHeight()
		logger.Info(fmt.Sprintf("processed %d/%d", lastBatchBlockHeight, lastBlockHeight))

		rows = append(rows, row{
			timestamp:                  lastBatchBlock.TransactionsBlock.Header.Timestamp(),
			blockHeight:                lastBatchBlockHeight,
			txCount:                    totalTxCount,
			medianBlockClosingTimeInMs: uint64(blockClosingTime.Current.Mean()),
		})

		//return lastBatchBlockHeight < 100000

		return lastBatchBlockHeight < lastBlockHeight
	}); err != nil {
		logger.Error("failed to process things", log.Error(err))
		os.Exit(1)
	}

	results := bytes.NewBufferString("")
	results.WriteString("timestamp,blockHeight,txCount,medianBlockClosingTime\n")
	for _, row := range rows {
		results.WriteString(fmt.Sprintf("%s,%d,%d,%d\n",
			time.Unix(0, int64(row.timestamp)).Format(time.RFC3339),
			row.blockHeight, row.txCount, row.medianBlockClosingTimeInMs))
	}

	if err := ioutil.WriteFile("out.csv", results.Bytes(), 0644); err != nil {
		logger.Error("could not write file", log.Error(err))
		os.Exit(1)
	} else {
		logger.Info("success")
	}
}
