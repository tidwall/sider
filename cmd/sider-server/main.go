package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"

	"github.com/tidwall/sider/server"
)

func main() {
	config, file, ok := parseArgs()
	if !ok {
		os.Exit(-1)
		return
	}
	if err := server.Start(&server.Options{
		Config: config,
		ConfigRewrite: func(config map[string]string) error {
			return mergeConfigFile(file, config)
		},
	}); err != nil {
		if !strings.HasPrefix(err.Error(), "Fatal config file error: ") {
			log.Print(err)
		}
	}
}

func parseArgs() (map[string]string, string, bool) {
	config := make(map[string]string)
	file := ""
	ln := 2
	for i := 1; i < len(os.Args); i++ {
		arg := os.Args[i]
		switch arg {
		default:
			if file == "" && !strings.HasPrefix(arg, "--") {
				file = arg
				var ok bool
				ln, ok = readConfigFile(file, config)
				if !ok {
					return nil, "", false
				}
				continue
			}
			i++
			var vals []string
			for ; i < len(os.Args); i++ {
				if strings.HasPrefix(os.Args[i], "--") {
					break
				}
				vals = append(vals, os.Args[i])
			}
			i += len(vals)
			if strings.HasPrefix(arg, "--") {
				arg = arg[2:]
			}
			switch arg {
			default:
				printBadConfig(arg, vals, ln)
				return nil, "", false
			case "port":
				if len(vals) != 1 {
					printBadConfig(arg, vals, ln)
					return nil, "", false
				}
				config["port"] = vals[0]
			case "bind":
				if len(vals) != 1 {
					printBadConfig(arg, vals, ln)
					return nil, "", false
				}
				config["port"] = vals[0]
			}
			ln++
		case "--help", "-h":
			printHelp()
			return nil, "", false
		case "--version", "-v":
			printVersion()
			return nil, "", false
		}
	}
	return config, file, true
}

func readConfigFile(file string, config map[string]string) (int, bool) {
	ln := 0
	f, err := os.Open(file)
	if err != nil {
		w := os.Stderr
		server.Log(w, '#', "Fatal error, can't open config file '"+file+"'")
		return 0, false
	}
	defer f.Close()
	rd := bufio.NewReader(f)
	for {
		ln++
		lineb, err := rd.ReadBytes('\n')
		if err != nil && err != io.EOF {
			w := os.Stderr
			server.Log(w, '#', "Fatal error, can't open config file '"+file+"'")
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
			printBadConfig(line, nil, ln)
			return 0, false
		case "port", "protected-mode", "bind", "requirepass":
			if val == "" {
				printBadConfig(line, nil, ln)
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

func printHelp() {
	base := path.Base(os.Args[0])
	os.Stdout.WriteString(strings.TrimSpace(`
	Usage: ./`+base+` [/path/to/sider.conf] [options]
	       ./`+base+` -v or --version
	       ./`+base+` -h or --help

	Examples:
	       ./`+base+` (run the server with default conf)
	       ./`+base+` /etc/sider/6379.conf
	       ./`+base+` --port 7777
	       ./`+base+` /etc/mysider.conf --loglevel verbose
	`) + "\n")
}

func printVersion() {
	os.Stdout.WriteString("Sider server v=999.999.999\n")
}
func printBadConfig(prop string, vals []string, ln int) {
	msg := prop
	for _, val := range vals {
		msg += " \"" + val + "\""
	}
	fmt.Fprintf(os.Stderr, `
*** FATAL CONFIG FILE ERROR ***
Reading the configuration file, at line %d
>>> '%s'
Bad directive or wrong number of arguments
`, ln, msg)
}
