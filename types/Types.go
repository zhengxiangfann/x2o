package types


type Config struct {
	Oracle struct {
		Host string `yaml:"host"`
		Port int `yaml:"port"`
		UserName string `yaml:"username"`
		Password string `yaml:"password"`
		Db string `yaml:"defaultdb"`
	}
	Hive struct {
		Host string `yaml:"host"`
		Port int `yaml:"port"`
		UserName string `yaml:"username"`
		Password string `yaml:"password"`
		Db string `yaml:"defaultdb"`
	}
}

type Config1 struct {
	Node []struct {
		Host string
		Port int
		UserName string
		Password string
		Db string
	}
}

