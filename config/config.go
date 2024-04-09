package config

import (
	"io/ioutil"
	"path/filepath"

	"gopkg.in/yaml.v2"
)

type ServerConfig struct {
	Port          string `yaml:"port"`
	Host          string `yaml:"host"`
	UDPPort       string `yaml:"udpport"`
	UDPBufferSize int    `yaml:"udpbuffersize"`
}

type DiskConfig struct {
	NumOfPartitions int    `yaml:"num_Of_Partitions"`
	Directory       string `yaml:"directory"`
}

type DBEngineConfig struct {
	LSMTreeConfig     LSMTreeConfig     `yaml:"lsmTree,inline"`
	BloomFilterConfig BloomFilterConfig `yaml:"bloom_filter_config,inline"`
	WalPath           string            `yaml:"walpath"`
}

type LSMTreeConfig struct {
	MaximumElement      int `yaml:"maximum_element"`
	CompactionFrequency int `yaml:"compaction_frequency"`
}

type BloomFilterConfig struct {
	Capacity  int     `yaml:"bloom_capacity"`
	ErrorRate float64 `yaml:"bloom_error_rate"`
}

type Config struct {
	ServerConfig   ServerConfig   `yaml:"server_config,inline"`
	DBEngineConfig DBEngineConfig `yaml:"db_engine_config,inline"`
	DiskConfig     DiskConfig     `yaml:"disk_config,inline"`
}

func Parse(filename string) (Config, error) {
	var config Config
	absfile, err := filepath.Abs(filename)
	if err != nil {
		return config, err
	}

	data, err := ioutil.ReadFile(absfile)
	if err != nil {
		return config, err
	}
	err = yaml.Unmarshal(data, &config)

	return config, err
}
