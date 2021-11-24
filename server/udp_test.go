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
		name 		string
		wantIP 		string
		wantCountry string
	} {
		{
			name: 		 "Get the right ip from production server.",
			wantIP: 	 "SETME",
			wantCountry: "US",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T){

			toAddr := "spv16.lbry.com"
			toPort := "50001"

			ip, country, err := UDPPing(toAddr, toPort)
			gotCountry := country
			if err != nil {
				log.Println(err)
			}

			res, err := exec.Command("dig", "@resolver4.opendns.com", "myip.opendns.com", "+short").Output()

			if err != nil {
				log.Println(err)
			}

			digIP := strings.TrimSpace(string(res))
			udpIP := ip.String()
			tt.wantIP = digIP

			gotIP := udpIP
			if gotIP != tt.wantIP {
				t.Errorf("got: '%s', want: '%s'\n", gotIP, tt.wantIP)
			}
			if gotCountry != tt.wantCountry {
				t.Errorf("got: '%s', want: '%s'\n", gotCountry, tt.wantCountry)
			}
		})
	}

}
