package server

import (
	"log"
	"os/exec"
	"strings"
	"testing"
)

// TestAddPeer tests the ability to add peers
func TestUDPPing(t *testing.T) {
	args := makeDefaultArgs()
	args.StartUDP = false

	tests := []struct {
		name string
		want string
	} {
		{
			name: "Get the right ip from production server.",
			want: "SETME",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T){

			toAddr := "spv16.lbry.com"
			toPort := "50001"

			ip, err := UDPPing(toAddr, toPort)
			if err != nil {
				log.Println(err)
			}

			res, err := exec.Command("dig", "@resolver4.opendns.com", "myip.opendns.com", "+short").Output()

			if err != nil {
				log.Println(err)
			}

			digIP := strings.TrimSpace(string(res))
			udpIP := ip.String()
			tt.want = digIP

			got1 := udpIP
			if got1 != tt.want {
				t.Errorf("got: '%s', want: '%s'\n", got1, tt.want)
			}
		})
	}

}
