package main

import (
	"fmt"
	"github.com/orbs-network/orbs-network-go/instrumentation/metric"
	"github.com/orbs-network/orbs-network-go/services/blockstorage/adapter/filesystem"
	"github.com/orbs-network/orbs-spec/types/go/primitives"
	"github.com/orbs-network/orbs-spec/types/go/protocol"
	"github.com/orbs-network/scribe/log"
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
	blockHeight primitives.BlockHeight
	txCount     uint64
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

	if err := persistence.ScanBlocks(1, 1000, func(first primitives.BlockHeight, page []*protocol.BlockPairContainer) (wantsMore bool) {
		totalTxCount := uint64(0)

		for _, block := range page {
			txs := block.TransactionsBlock.SignedTransactions
			totalTxCount += uint64(len(txs))
		}

		lastBatchHeight := page[len(page)-1].TransactionsBlock.Header.BlockHeight()
		logger.Info(fmt.Sprintf("processed %d/%d", lastBatchHeight, lastBlockHeight))

		rows = append(rows, row{
			blockHeight: lastBatchHeight,
			txCount:     totalTxCount,
		})

		return lastBatchHeight < 100000

		return lastBatchHeight < lastBlockHeight
	}); err != nil {
		logger.Error("failed to process things", log.Error(err))
		os.Exit(1)
	}

	for _, row := range rows {
		fmt.Println(fmt.Sprintf("%d,%d", row.blockHeight, row.txCount))
	}
}
