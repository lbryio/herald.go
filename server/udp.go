package server

import (
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	pb "github.com/lbryio/hub/protobuf/go"
)

const maxBufferSize = 1024

// genesis blocktime (which is actually wrong)
// magic constant for the UDPPing protocol. The above comment is taken from
// the python code this was implemented off of.
// https://github.com/lbryio/lbry-sdk/blob/7d49b046d44a4b7067d5dc1d6cd65ff0475c71c8/lbry/wallet/server/udp.py#L12
const magic = 1446058291
const protocolVersion = 1
const defaultFlags = 0b00000000
const availableFlag = 0b00000001

// SPVPing is a struct for the format of how to ping another hub over udp.
// format b'!lB64s'
type SPVPing struct {
	magic   uint32
	version byte
	padding []byte //64
}

// SPVPong is a struct for the return pong from another hub server.
// format b'!BBL32s4sH'
type SPVPong struct {
	protocolVersion byte
	flags           byte
	height          uint32
	tip             []byte // 32
	srcAddrRaw      []byte // 4
	country         uint16
}

// encodeSPVPing creates a slice of bytes to ping another hub with
// over udp.
func encodeSPVPing() []byte {
	data := make([]byte, 69)

	binary.BigEndian.PutUint32(data, magic)
	data[4] = protocolVersion

	return data
}

// decodeSPVPing takes a slice of bytes and decodes an SPVPing struct from them.
func decodeSPVPing(data []byte) *SPVPing {
	if len(data) < 69 {
		return nil
	}

	parsedMagic := binary.BigEndian.Uint32(data)
	parsedProtocalVersion := data[4]
	return &SPVPing{
		magic:   parsedMagic,
		version: parsedProtocalVersion,
	}
}

// Encode is a function for SPVPong structs to encode them into bytes for
// sending over udp.
func (pong *SPVPong) Encode() []byte {
	data := make([]byte, 44)

	data[0] = pong.protocolVersion
	data[1] = pong.flags
	binary.BigEndian.PutUint32(data[2:], pong.height)
	copy(data[6:], pong.tip)
	copy(data[38:], pong.srcAddrRaw)
	binary.BigEndian.PutUint16(data[42:], pong.country)

	return data
}

// makeSPVPong creates an SPVPong struct according to given parameters.
func makeSPVPong(flags int, height int, tip []byte, sourceAddr string, country string) *SPVPong {
	byteAddr := EncodeAddress(sourceAddr)
	var countryInt int32
	var ok bool
	if countryInt, ok = pb.Location_Country_value[country]; !ok {
		countryInt = int32(pb.Location_UNKNOWN_COUNTRY)
	}
	return &SPVPong{
		protocolVersion: protocolVersion,
		flags:           byte(flags),
		height:          uint32(height),
		tip:             tip,
		srcAddrRaw:      byteAddr,
		country:         uint16(countryInt),
	}
}

// decodeSPVPong takes a slice of bytes and decodes an SPVPong struct
// from it.
func decodeSPVPong(data []byte) *SPVPong {
	if len(data) < 44 {
		return nil
	}

	parsedProtocalVersion := data[0]
	flags := data[1]
	height := binary.BigEndian.Uint32(data[2:])
	tip := make([]byte, 32)
	copy(tip, data[6:38])
	srcRawAddr := make([]byte, 4)
	copy(srcRawAddr, data[38:42])
	country := binary.BigEndian.Uint16(data[42:])
	return &SPVPong{
		protocolVersion: parsedProtocalVersion,
		flags:           flags,
		height:          height,
		tip:             tip,
		srcAddrRaw:      srcRawAddr,
		country:         country,
	}
}

// EncodeAddress takes an ipv4 address and encodes it into bytes for the hub
// Ping/Pong protocol.
func EncodeAddress(addr string) []byte {
	parts := strings.Split(addr, ".")

	if len(parts) != 4 {
		return []byte{}
	}

	data := make([]byte, 4)
	for i, part := range parts {
		x, err := strconv.Atoi(part)
		if err != nil || x > 255 {
			return []byte{}
		}
		data[i] = byte(x)
	}

	return data
}

// DecodeAddress gets the string ipv4 address from an SPVPong struct.
func (pong *SPVPong) DecodeAddress() net.IP {
	return net.IPv4(
		pong.srcAddrRaw[0],
		pong.srcAddrRaw[1],
		pong.srcAddrRaw[2],
		pong.srcAddrRaw[3],
	)
}

func (pong *SPVPong) DecodeCountry() string {
	return pb.Location_Country_name[int32(pong.country)]
}

func (pong *SPVPong) DecodeProtocolVersion() int {
	return int(pong.protocolVersion)
}

func (pong *SPVPong) DecodeHeight() int {
	return int(pong.height)
}

func (pong *SPVPong) DecodeTip() []byte {
	return pong.tip
}

func (pong *SPVPong) DecodeFlags() byte {
	return pong.flags
}

// UDPPing sends a ping over udp to another hub and returns the ip address of
// this hub.
func UDPPing(ip, port string) (*SPVPong, error) {
	address := ip + ":" + port
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}

	defer conn.Close()

	_, err = conn.Write(encodeSPVPing())
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, maxBufferSize)
	deadline := time.Now().Add(time.Second)
	err = conn.SetReadDeadline(deadline)
	if err != nil {
		return nil, err
	}
	n, _, err := conn.ReadFromUDP(buffer)
	if err != nil {
		return nil, err
	}

	pong := decodeSPVPong(buffer[:n])

	if pong == nil {
		return nil, fmt.Errorf("Pong decoding failed")
	}

	return pong, nil
}

// UDPServer is a goroutine that starts an udp server that implements the hubs
// Ping/Pong protocol to find out about each other without making full TCP
// connections.
func UDPServer(args *Args) error {
	address := ":" + args.Port
	tip := make([]byte, 32)
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}

	defer conn.Close()

	buffer := make([]byte, maxBufferSize)
	for {
		//TODO verify ping
		_, addr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			return err
		}

		sAddr := addr.IP.String()
		pong := makeSPVPong(defaultFlags|availableFlag, 0, tip, sAddr, args.Country)
		data := pong.Encode()

		_, err = conn.WriteToUDP(data, addr)
		if err != nil {
			return err
		}
	}
}
