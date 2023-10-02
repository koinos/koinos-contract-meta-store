package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path"
	"runtime"
	"syscall"

	"google.golang.org/protobuf/proto"

	"github.com/dgraph-io/badger/v3"
	"github.com/koinos/koinos-contract-meta-store/internal/metastore"
	log "github.com/koinos/koinos-log-golang"
	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-proto-golang/koinos/broadcast"
	"github.com/koinos/koinos-proto-golang/koinos/contract_meta_store"
	"github.com/koinos/koinos-proto-golang/koinos/protocol"
	"github.com/koinos/koinos-proto-golang/koinos/rpc"
	contract_meta_store_rpc "github.com/koinos/koinos-proto-golang/koinos/rpc/contract_meta_store"
	"github.com/mr-tron/base58"

	util "github.com/koinos/koinos-util-golang"
	flag "github.com/spf13/pflag"
)

const (
	basedirOption    = "basedir"
	amqpOption       = "amqp"
	instanceIDOption = "instance-id"
	logLevelOption   = "log-level"
	logDirOption     = "log-dir"
	logColorOption   = "log-color"
	resetOption      = "reset"
	jobsOption       = "jobs"
	versionOption    = "version"
)

const (
	basedirDefault    = ".koinos"
	amqpDefault       = "amqp://guest:guest@localhost:5672/"
	instanceIDDefault = ""
	logLevelDefault   = "info"
	logColorDefault   = false
	resetDefault      = false
)

const (
	metaStoreRPC = "contract_meta_store"
	blockAccept  = "koinos.block.accept"
	appName      = "contract_meta_store"
)

// Version display values
const (
	DisplayAppName = "Koinos Contract Meta Store"
	Version        = "v1.1.0"
)

// Gets filled in by the linker
var Commit string

func main() {
	jobsDefault := runtime.NumCPU()

	var baseDir string

	baseDirPtr := flag.StringP(basedirOption, "d", basedirDefault, "Koinos base directory")
	amqp := flag.StringP(amqpOption, "a", "", "AMQP server URL")
	reset := flag.BoolP(resetOption, "r", resetDefault, "Reset the database")
	instanceID := flag.StringP(instanceIDOption, "i", instanceIDDefault, "The instance ID to identify this service")
	logLevel := flag.StringP(logLevelOption, "l", logLevelDefault, "The log filtering level (debug, info, warning, error)")
	logDir := flag.String(logDirOption, "", "The logging directory")
	logColor := flag.Bool(logColorOption, logColorDefault, "Log color toggle")
	jobs := flag.IntP(jobsOption, "j", jobsDefault, "Number of RPC jobs to run")
	version := flag.BoolP(versionOption, "v", false, "Print version and exit")

	flag.Parse()

	if *version {
		fmt.Println(makeVersionString())
		os.Exit(0)
	}

	baseDir, err := util.InitBaseDir(*baseDirPtr)
	if err != nil {
		fmt.Printf("Could not initialize base directory '%v'\n", baseDir)
		os.Exit(1)
	}

	yamlConfig := util.InitYamlConfig(baseDir)

	*amqp = util.GetStringOption(amqpOption, amqpDefault, *amqp, yamlConfig.ContractMetaStore, yamlConfig.Global)
	*logLevel = util.GetStringOption(logLevelOption, logLevelDefault, *logLevel, yamlConfig.ContractMetaStore, yamlConfig.Global)
	*logDir = util.GetStringOption(logDirOption, *logDir, *logDir, yamlConfig.ContractMetaStore, yamlConfig.Global)
	*logColor = util.GetBoolOption(logColorOption, logColorDefault, *logColor, yamlConfig.ContractMetaStore, yamlConfig.Global)
	*instanceID = util.GetStringOption(instanceIDOption, util.GenerateBase58ID(5), *instanceID, yamlConfig.ContractMetaStore, yamlConfig.Global)
	*reset = util.GetBoolOption(resetOption, resetDefault, *reset, yamlConfig.ContractMetaStore, yamlConfig.Global)
	*jobs = util.GetIntOption(jobsOption, jobsDefault, *jobs, yamlConfig.ContractMetaStore, yamlConfig.Global)

	if len(*logDir) > 0 && !path.IsAbs(*logDir) {
		*logDir = path.Join(util.GetAppDir(baseDir, appName), *logDir)
	}

	err = log.InitLogger(appName, *instanceID, *logLevel, *logDir, *logColor)
	if err != nil {
		fmt.Printf("Invalid log-level: %s. Please choose one of: debug, info, warning, error", *logLevel)
		os.Exit(1)
	}

	if *jobs < 1 {
		log.Errorf("Option '%v' must be greater than 0 (was %v)", jobsOption, *jobs)
		os.Exit(1)
	}

	log.Info(makeVersionString())

	// Costruct the db directory and ensure it exists
	dbDir := path.Join(util.GetAppDir((baseDir), appName), "db")
	err = util.EnsureDir(dbDir)
	if err != nil {
		log.Errorf("Could not create database folder %v", dbDir)
		os.Exit(1)
	}

	log.Infof("Opening database at %s", dbDir)

	var opts = badger.DefaultOptions(dbDir)
	opts.Logger = metastore.KoinosBadgerLogger{}
	var backend = metastore.NewBadgerBackend(opts)

	// Reset backend if requested
	if *reset {
		log.Info("Resetting database")
		err := backend.Reset()
		if err != nil {
			log.Errorf("Error resetting database: %s\n", err.Error())
			os.Exit(1)
		}
	}

	requestHandler := koinosmq.NewRequestHandler(*amqp, uint(*jobs), koinosmq.ExponentialBackoff)

	metaStore := metastore.NewContractMetaStore(backend)

	requestHandler.SetRPCHandler(metaStoreRPC, func(rpcType string, data []byte) ([]byte, error) {
		request := &contract_meta_store_rpc.ContractMetaStoreRequest{}
		response := &contract_meta_store_rpc.ContractMetaStoreResponse{}

		err := proto.Unmarshal(data, request)

		if err != nil {
			log.Warnf("Received malformed request: %v", data)
		} else {
			log.Debugf("Received RPC request: %s", request.String())
			switch v := request.Request.(type) {
			case *contract_meta_store_rpc.ContractMetaStoreRequest_GetContractMeta:
				if contractMeta, err := metaStore.GetContractMeta(v.GetContractMeta.ContractId); err == nil {
					r := &contract_meta_store_rpc.GetContractMetaResponse{Meta: contractMeta}
					response.Response = &contract_meta_store_rpc.ContractMetaStoreResponse_GetContractMeta{GetContractMeta: r}
				}
			default:
				err = errors.New("unknown request")
			}
		}

		if err != nil {
			e := &rpc.ErrorResponse{Message: string(err.Error())}
			response.Response = &contract_meta_store_rpc.ContractMetaStoreResponse_Error{Error: e}
		}

		return proto.Marshal(response)
	})

	requestHandler.SetBroadcastHandler(blockAccept, func(topic string, data []byte) {
		submission := &broadcast.BlockAccepted{}

		if err := proto.Unmarshal(data, submission); err != nil {
			log.Warnf("Unable to parse koinos.block.accept broadcast: %v", data)
			return
		}

		log.Debugf("Received broadcasted block - %s", util.BlockString(submission.Block))

		// Iterate through the operations and look for upload contract abi
		for _, tx := range submission.Block.Transactions {
			for _, op := range tx.Operations {
				switch v := op.Op.(type) {
				case *protocol.Operation_UploadContract:
					log.Infof("Adding metadata for contract - %s", base58.Encode(v.UploadContract.ContractId))
					msi := &contract_meta_store.ContractMetaItem{Abi: v.UploadContract.Abi}
					if err := metaStore.AddMeta(v.UploadContract.ContractId, msi); err != nil {
						log.Warnf("Error adding contract metadata: %s", err)
					}
				}
			}
		}
	})

	ctx, ctxCancel := context.WithCancel(context.Background())
	requestHandler.Start(ctx)

	// Wait for a SIGINT or SIGTERM signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Info("Shutting down node...")
	ctxCancel()
	backend.Close()
}

func makeVersionString() string {
	commitString := ""
	if len(Commit) >= 8 {
		commitString = fmt.Sprintf("(%s)", Commit[0:8])
	}

	return fmt.Sprintf("%s %s %s", DisplayAppName, Version, commitString)
}
