package main

import (
	"flag"

	lsmtree "github.com/jiteshchawla1511/KryptonDB/LSM_Tree"
	"github.com/jiteshchawla1511/KryptonDB/config"
	dbengine "github.com/jiteshchawla1511/KryptonDB/dbEngine"
	diskstore "github.com/jiteshchawla1511/KryptonDB/diskStore"
	"github.com/jiteshchawla1511/KryptonDB/server"
	"github.com/jiteshchawla1511/KryptonDB/wal"
)

func initServerConfig(configFile string) (config.Config, error) {
	serverConfig, err := config.Parse(configFile)
	if err != nil {
		return serverConfig, err
	}

	if serverConfig.ServerConfig.Port == "" {
		serverConfig.ServerConfig.Port = server.DefaultTCPPort
	}

	if serverConfig.ServerConfig.Host == "" {
		serverConfig.ServerConfig.Host = server.DefaultHost
	}

	if serverConfig.ServerConfig.UDPPort == "" {
		serverConfig.ServerConfig.UDPPort = server.DefaultUDPPort
	}

	if serverConfig.ServerConfig.UDPBufferSize == 0 {
		serverConfig.ServerConfig.UDPBufferSize = server.DefaultUDPBufferSize
	}

	if serverConfig.DBEngineConfig.WalPath == "" {
		serverConfig.DBEngineConfig.WalPath = wal.DefaultWalPath
	}

	if serverConfig.DBEngineConfig.LSMTreeConfig.MaximumElement == 0 {
		serverConfig.DBEngineConfig.LSMTreeConfig.MaximumElement = lsmtree.MaximumElement
	}

	if serverConfig.DBEngineConfig.LSMTreeConfig.CompactionFrequency == 0 {
		serverConfig.DBEngineConfig.LSMTreeConfig.CompactionFrequency = lsmtree.CompactionFrequency
	}

	if serverConfig.DBEngineConfig.BloomFilterConfig.ErrorRate == 0 {
		serverConfig.DBEngineConfig.BloomFilterConfig.ErrorRate = lsmtree.BloomErrorRate
	}

	if serverConfig.DBEngineConfig.BloomFilterConfig.Capacity == 0 {
		serverConfig.DBEngineConfig.BloomFilterConfig.Capacity = lsmtree.BloomFilterCapacity
	}

	if serverConfig.DiskConfig.NumOfPartitions == 0 {
		serverConfig.DiskConfig.NumOfPartitions = diskstore.DefaultNumOfPartitions
	}

	if serverConfig.DiskConfig.Directory == "" {
		serverConfig.DiskConfig.Directory = diskstore.DefaultDirectory
	}

	return serverConfig, nil
}

func main() {

	var configFile string
	flag.StringVar(&configFile, "config", "config.yaml", "/Users/jiteshchawla/KDB/KryptonDB/config.yaml")
	flag.Parse()

	serverConfig, err := initServerConfig(configFile)

	if err != nil {
		panic(err)
	}

	lsm_tree_options := lsmtree.LSMTreeOptions{
		MaximumElement:   serverConfig.DBEngineConfig.LSMTreeConfig.MaximumElement,
		CompactionPeriod: serverConfig.DBEngineConfig.LSMTreeConfig.CompactionFrequency,
		BloomFilterOptions: lsmtree.CustomBloomFilterOptions{
			ErrorRate: serverConfig.DBEngineConfig.BloomFilterConfig.ErrorRate,
			Capacity:  serverConfig.DBEngineConfig.BloomFilterConfig.Capacity,
		},
	}

	lsm_tree := lsmtree.InitLsmTree(lsm_tree_options)

	disk_store_opts := diskstore.DiskStoreOpts{
		NumOfPartitions: serverConfig.DiskConfig.NumOfPartitions,
		Directory:       serverConfig.DiskConfig.Directory,
	}

	disk_store := diskstore.NewDisk(disk_store_opts)

	server := server.Server{
		Port:          serverConfig.ServerConfig.Port,
		Host:          serverConfig.ServerConfig.Host,
		UDPPort:       serverConfig.ServerConfig.UDPPort,
		UDPBufferSize: serverConfig.ServerConfig.UDPBufferSize,
		DBEngine: &dbengine.DBEngine{
			Lsmtree: lsm_tree,
			WAL:     wal.InitWal(serverConfig.DBEngineConfig.WalPath),
			Store:   disk_store,
		},
	}
	server.Start()

}
