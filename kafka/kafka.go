package kafka

import (
	"encoding/binary"
	"fmt"
	"net"
	//"bytes"
	"bytes"
	"time"

	"hash/crc32"
)

const (
	ErrUnknown              = -1
	ErrNone                 = 0
	OffsetOutOfRange        = 1
	CorruptMessage          = 2
	UnknownTopicOrPartition = 3
	InvalidFetchSize        = 4
	LeaderNotAvailable      = 5
	NotLeaderForPartition   = 6
	RequestTimedOut         = 7
	// TODO: add the rest: http://kafka.apache.org/protocol.html#protocol_error_codes
	//       make them meaningful?
)

// API Keys  http://kafka.apache.org/protocol.html#protocol_api_keys
const (
	ApiProduce      = 0
	ApiFetch        = 1
	ApiOffsets      = 2
	ApiMetaData     = 3
	ApiLeaderAndIsr = 4

	ApiVersions = 18
)

const (
	ApiVersionZero = 0
	ApiVersionOne  = 1
	ApiVersionTwo  = 2
)

type Client struct {
	broker string
	conn   *net.Conn
}

// first of all list meta data
func (c Client) MetaData() {
	// can be made to any of the brokers
	// is considered valid until either
	// (1) socket error indicate client cannot communicate with particular broker
	// (2) an error code in the response, indicating broker no longers hosts the required partition
}

// http://kafka.apache.org/protocol.html
func Connect(brokers []string) (Client, error) {

	// NOTEs:
	// * TCP happier with persistent connection
	// * Typically 1:1 connection between client and broker (can be multiple brokers)
	// * Server guarantees requests processed in-order for single TCP connection
	// * Recommended to use NIO for performance
	// * If the request size exceeds server limit, socket will be disconnected

	//
	// bootstrapping
	// cycle through available brokers until it can connect to one
	// FIXME: handle multiple brokers
	conn, err := net.Dial("tcp", brokers[0])
	if err != nil {
		fmt.Printf("Cannot establish connection to kafka broker : %v\n", err)
		return Client{}, err
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(5 * time.Second))

	fmt.Println("Connected")

	//request := new(bytes.Buffer)
	//binary.Write(request, binary.BigEndian, int16(ApiVersions))
	//binary.Write(request, binary.BigEndian, int16(ApiVersionZero))
	//// correlation ID
	//binary.Write(request, binary.BigEndian, int32(1234))
	//binary.Write(request, binary.BigEndian, int32(-1)) // nullable client Id ?

	type versionrequest struct {
	}

	type message struct {
		apiKey        int16
		apiVersion    int16
		correlationId int32
		clientIdLen   int16
		clientId      []byte
		message       interface{}
	}

	clientId := "testclient"
	fmt.Printf("Clientid is %d long\n", len(clientId))
	m := message{
		int16(ApiVersions),
		int16(ApiVersionZero),
		int32(1),
		int16(len(clientId)),
		[]byte(clientId),
		versionrequest{},
	}
	fmt.Printf("Message is %v\n", m)

	type messageSet struct {
		offset     int64
		size       int32
		crc32      int32
		magicByte  int8
		attributes int8
		keyLen     int32
		key        []byte
	}

	//key := "fooKey"
	ms := messageSet{
		int64(-1),
		int32(-1),
		int32(-1),
		int8(1), // magic byte always 1
		int8(0), // no compression, create timestamp
		int32(-1),
		[]byte{},
	}

	// FIXME: needs to be wrapped in message set
	// message set

	buff := new(bytes.Buffer)

	binary.Write(buff, binary.BigEndian, ms.magicByte)
	binary.Write(buff, binary.BigEndian, ms.attributes)
	binary.Write(buff, binary.BigEndian, ms.keyLen)
	binary.Write(buff, binary.BigEndian, ms.key)

	// message
	// size
	binary.Write(buff, binary.BigEndian, int32(8+len(clientId)))
	binary.Write(buff, binary.BigEndian, m.apiKey)
	binary.Write(buff, binary.BigEndian, m.apiVersion)
	binary.Write(buff, binary.BigEndian, m.correlationId)
	binary.Write(buff, binary.BigEndian, m.clientIdLen)
	binary.Write(buff, binary.BigEndian, m.clientId)
	//binary.Write(buff, binary.BigEndian, byte('\r'))
	//binary.Write(buff, binary.BigEndian, byte('\n'))

	fmt.Printf("%v\n", buff.Bytes())

	ms.crc32 = int32(crc32.ChecksumIEEE(buff.Bytes()))

	t := new(bytes.Buffer)
	//binary.Write(t, binary.BigEndian, ms.offset)
	binary.Write(t, binary.BigEndian, int32(len(buff.Bytes())+4))
	binary.Write(t, binary.BigEndian, ms.crc32)

	conn.Write(t.Bytes())
	conn.Write(buff.Bytes())

	//	// request
	////	conn.Write([]byte{4, 't','e','s','t',})
	//	binary.Write(request, binary.BigEndian, int16(0)) // array of topics, in this case empty array means all
	//	binary.Write(request, binary.BigEndian, int32(0))
	//	binary.Write(request, binary.BigEndian, int64(0))
	//	binary.Write(request, binary.BigEndian, int32(0))

	//fmt.Printf("Request : %v of %d bytes\n", request.String(), request.Len())
	//
	//size := new(bytes.Buffer)
	//binary.Write(request, binary.BigEndian, int32(request.Len()))
	//conn.Write(size.Bytes())
	//conn.Write(request.Bytes())
	//
	//

	fmt.Println("Sent Request")

	response := make([]byte, 50)
	err = binary.Read(conn, binary.BigEndian, response)
	if err != nil {
		fmt.Printf("Error reading from Kafka :%v \n", err)
	}

	fmt.Printf("Response: %v\n", response)

	//response, err := bufio.NewReader(conn).ReadString('\n')
	//if err != nil {
	//	fmt.Printf("Can't read : %v\n", err)
	//} else {
	//	fmt.Printf("response :%v\n", response)
	//}

	return Client{}, nil //Client{brokers[0], &conn}
}
