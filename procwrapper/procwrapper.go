package procwrapper

import (
	"fmt"
	"os"
	"strings"
	"os/exec"
)

func main() {
	var opt string

	// args to pass to curl
	var args []string
	for i, v := range os.Args {
		switch i {
		case 1:
			opt = v
		}

		if strings.Compare("--", v) == 0 {
			args = os.Args[i+1:]
			break
		}
	}

	// Useful in this case to augment call to curl (adding auth / custom headers)
	args = append(args, "-H", fmt.Sprintf("X-Opt: %s", opt))

	// Call curl
	cmd := exec.Command("curl", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fmt.Printf("Error invoking curl: %v\n", err)
	}

	cmd.Wait()
}

