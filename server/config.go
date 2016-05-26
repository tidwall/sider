package server

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
)

type config struct {
	port          int
	bind          string
	bindIsLocal   bool
	protectedMode bool
	requirepass   string

	kvm  map[string]string
	file string
}

type cfgerr struct {
	message  string
	property string
	value    string
}

func (err *cfgerr) Error() string {
	return fmt.Sprintf("Fatal config file error: '%s \"%s\"': %s", err.property, err.value, err.message)
}

func fillBoolConfigOption(configMap map[string]string, configKey string, defaultValue bool) {
	switch strings.ToLower(configMap[configKey]) {
	default:
		if defaultValue {
			configMap[configKey] = "yes"
		} else {
			configMap[configKey] = "no"
		}
	case "yes", "no":
		configMap[configKey] = strings.ToLower(configMap[configKey])
	}
}

// fillOptions takes makes sure that the options are sane and
// set to defaults.
func fillOptions(options *Options) (
	nopts *Options,
	configMap map[string]string,
	configFile string,
	ok bool,
) {
	if options == nil {
		options = &Options{}
	}
	if options.LogWriter == nil {
		options.LogWriter = os.Stderr
	}
	if options.AppendOnlyPath == "" {
		options.AppendOnlyPath = "appendonly.aof"
	}
	if options.AppName == "" {
		options.AppName = "Sider"
	}
	if options.Version == "" {
		options.Version = "999.999.9999"
	}
	if len(options.Args) > 0 {
		configMap, configFile, ok = loadConfigArgs(options)
		if !ok {
			return options, nil, "", false
		}
	}
	if configMap == nil {
		configMap = map[string]string{}
	}
	// force valid strings into each config property
	s := func(s string) string {
		return s
	}
	configMap["bind"] = s(configMap["bind"])
	configMap["port"] = s(configMap["port"])
	configMap["protected-mode"] = s(configMap["protected-mode"])
	configMap["requirepass"] = s(configMap["requirepass"])

	// defaults
	if configMap["port"] == "" {
		configMap["port"] = "6379"
	}
	fillBoolConfigOption(configMap, "protected-mode", true)
	return options, configMap, configFile, true
}

func fillConfig(configMap map[string]string, configFile string) (*config, error) {
	cfg := &config{}
	cfg.file = configFile
	cfg.kvm = configMap
	n, err := strconv.ParseUint(configMap["port"], 10, 16)
	if err != nil {
		return nil, &cfgerr{"Invalid port", "port", configMap["port"]}
	}
	cfg.port = int(n)
	cfg.bind = strings.ToLower(configMap["bind"])
	cfg.bindIsLocal = cfg.bind == "" || cfg.bind == "127.0.0.1" || cfg.bind == "::1" || cfg.bind == "localhost"
	switch strings.ToLower(configMap["protected-mode"]) {
	default:
		return nil, &cfgerr{"argument must be 'yes' or 'no'", "protected-mode", configMap["protected-mode"]}
	case "yes":
		cfg.protectedMode = true
	case "no":
		cfg.protectedMode = false
	}
	cfg.requirepass = configMap["requirepass"]
	return cfg, nil
}

func loadConfigArgs(options *Options) (config map[string]string, file string, ok bool) {
	config = make(map[string]string)
	ln := 2
	for i := 0; i < len(options.Args); i++ {
		arg := options.Args[i]
		switch arg {
		default:
			if file == "" && !strings.HasPrefix(arg, "--") {
				file = arg
				var ok bool
				ln, ok = readConfigFile(file, config, options)
				if !ok {
					return nil, "", false
				}
				continue
			}
			i++
			var vals []string
			for ; i < len(options.Args); i++ {
				if strings.HasPrefix(options.Args[i], "--") {
					break
				}
				vals = append(vals, options.Args[i])
			}
			i += len(vals)
			if strings.HasPrefix(arg, "--") {
				arg = arg[2:]
			}
			switch arg {
			default:
				printBadConfig(arg, vals, ln, options)
				return nil, "", false
			case "port":
				if len(vals) != 1 {
					printBadConfig(arg, vals, ln, options)
					return nil, "", false
				}
				config["port"] = vals[0]
			case "bind":
				if len(vals) != 1 {
					printBadConfig(arg, vals, ln, options)
					return nil, "", false
				}
				config["port"] = vals[0]
			}
			ln++
		case "--help", "-h":
			printHelp(options)
			return nil, "", false
		case "--version", "-v":
			printVersion(options)
			return nil, "", false
		}
	}
	return config, file, true
}

func readConfigFile(file string, config map[string]string, options *Options) (int, bool) {
	ln := 0
	f, err := os.Open(file)
	if err != nil {
		log(options.LogWriter, '#', "Fatal error, can't open config file '"+file+"'")
		return 0, false
	}
	defer f.Close()
	rd := bufio.NewReader(f)
	for {
		ln++
		lineb, err := rd.ReadBytes('\n')
		if err != nil && err != io.EOF {
			log(options.LogWriter, '#', "Fatal error, can't open config file '"+file+"'")
			return 0, false
		}
		if len(lineb) == 0 {
			break
		}

		line := strings.TrimSpace(string(lineb))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		var arg, val string
		sp := strings.Index(line, " ")
		if sp == -1 {
			arg = line
			val = ""
		} else {
			arg = line[:sp]
			val = strings.TrimSpace(line[sp:])
		}
		config[arg] = val
		switch arg {
		default:
			printBadConfig(line, nil, ln, options)
			return 0, false
		case "port", "protected-mode", "bind", "requirepass":
			if val == "" {
				printBadConfig(line, nil, ln, options)
				return 0, false
			}
		}
		if err == io.EOF {
			break
		}

	}
	return ln + 1, true
}

func mergeConfigFile(file string, config map[string]string) error {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
nextkey:
	for k, v := range config {
		for i, line := range lines {
			line = strings.TrimSpace(line)
			if line == k || strings.HasPrefix(line, k+" ") {
				if v == "" {
					// erase line
					lines = append(lines[:i], lines[i+1:]...)
				} else {
					// modify line
					lines[i] = k + " " + v
				}
				continue nextkey
			}
		}
		if v != "" {
			lines = append(lines, k+" "+v)
		}
	}
	return ioutil.WriteFile(file, []byte(strings.Join(lines, "\n")), 0644)
}

func printHelp(options *Options) {
	base := path.Base(os.Args[0])
	io.WriteString(options.LogWriter, strings.TrimSpace(`
	Usage: ./`+base+` [/path/to/`+strings.ToLower(options.AppName)+`.conf] [options]
	       ./`+base+` -v or --version
	       ./`+base+` -h or --help

	Examples:
	       ./`+base+` (run the server with default conf)
	       ./`+base+` /etc/`+strings.ToLower(options.AppName)+`/9851.conf
	       ./`+base+` --port 7777
	       ./`+base+` /etc/my`+strings.ToLower(options.AppName)+`.conf --loglevel verbose
	`)+"\n")
}

func printVersion(options *Options) {
	fmt.Fprintf(options.LogWriter, "%s server v=%s", options.AppName, options.Version)
}

func printBadConfig(prop string, vals []string, ln int, options *Options) {
	msg := prop
	for _, val := range vals {
		msg += " \"" + val + "\""
	}
	fmt.Fprintf(options.LogWriter, `
*** FATAL CONFIG FILE ERROR ***
Reading the configuration file, at line %d
>>> '%s'
Bad directive or wrong number of arguments
`, ln, msg)
}
