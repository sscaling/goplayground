package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"testing"
	"time"
)

/*
https://kafka.apache.org/protocol
https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets

// Probably should use Marshall rather than Encode
// Probably want to be consistent with methods, e.g. func Marshall() []bytes {...}
*/

//FIXME: error handling

type requestBodyHeader struct {
	crc        int32
	magicByte  int8
	attributes int8
	timestamp  int64
}

type requestBody struct {
	key   kafkaBytes
	value kafkaBytes
}

func (r *requestBody) Encode() []byte {
	b := new(bytes.Buffer)

	writeBigEndian(b, requestBodyHeader{
		magicByte:  1,
		attributes: 0,
		timestamp:  time.Now().Unix(),
	})

	r.key.Encode(b)
	r.value.Encode(b)

	// need to build message first, to calculate CRC32
	payload := b.Bytes()
	crc := crc32.ChecksumIEEE(payload)

	// crc is inserted at the beginning of the message
	binary.BigEndian.PutUint32(payload, crc)

	return payload
}

func NewRequestBody(key, value []byte) requestBody {
	return requestBody{
		kafkaBytes(key),
		kafkaBytes(value),
	}
}

type requestHeader struct {
	size          int32
	apiKey        int16
	apiVersion    int16
	correlationId int32
}

type requestMessage struct {
	requestHeader

	clientId kafkaString

	messageSets []requestBody
}

func (r *requestMessage) Encode(w io.Writer) {
	writeBigEndian(w, r.requestHeader)

	r.clientId.Encode(w)

	// Write each message in the message set
	for offset, body := range r.messageSets {
		b := body.Encode()

		writeBigEndian(w, int64(offset+1))
		k := kafkaBytes(b)
		k.Encode(w)
	}
}

type kafkaString []byte

func (k *kafkaString) Encode(w io.Writer) {
	if l := len(*k); l > 0 {
		writeBigEndian(w, int16(l))
		writeBigEndian(w, *k)
	} else {
		writeBigEndian(w, int16(-1))
	}
}

type kafkaBytes []byte

func (k *kafkaBytes) Encode(w io.Writer) {
	if l := len(*k); l > 0 {
		writeBigEndian(w, int32(l))
		writeBigEndian(w, *k)
	} else {
		writeBigEndian(w, int32(-1))
	}
}

type Request interface {
	Encode() kafkaBytes
}

type MetaDataRequest struct {
	topicname string
}

func (r MetaDataRequest) Encode() kafkaBytes {
	// FIXME: what is a better way of doing this?
	b := new(bytes.Buffer)
	s := kafkaString([]byte(r.topicname))
	s.Encode(b)
	return kafkaBytes(b.Bytes())
}

type VersionsRequest struct{}

func (r VersionsRequest) Encode() kafkaBytes {
	return kafkaBytes(nil)
}

func connect(t *testing.T) net.Conn {
	conn, err := net.Dial("tcp", "kafka:9092")
	if err != nil {
		fmt.Printf("Cannot establish connection to kafka broker : %v\n", err)
		t.FailNow()
	}

	conn.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Println("Connected")

	return conn
}

func TestKafkaRead(t *testing.T) {
	fmt.Println("start")

	conn := connect(t)
	defer conn.Close()

	// Bad  [0 0 0 48 0 18 0 0 0 0 0 15 255 255 0 0 0 0 0 0 0 1 (<offset) 0 0 0 22 (<msgSize) 197 143 227 87 1 0 0 0 0 0 89 21 148 229 255 255 255 255 255 255 255 255]
	// Good [         0 18 0 0 0 0 0 13 255 255 0 0 0 0 0 0 0 1 (<offset) 0 0 0 22 (<msgSize) 41 54 49 17    1 0 0 0 0 0 89 21 146 254 255 255 255 255 255 255 255 255]
	r := requestMessage{
		requestHeader{
			apiKey:        ApiVersions,
			apiVersion:    ApiVersionZero,
			correlationId: 15,
		},
		kafkaString(nil),
		[]requestBody{
			NewRequestBody(nil, MetaDataRequest{"topiclogs"}.Encode()),
			//NewRequestBody(nil, VersionsRequest{}.Encode()),
		},
	}

	buff := new(bytes.Buffer)
	r.Encode(buff)

	fmt.Printf("len ? %d\n", buff.Len())

	bytes := buff.Bytes()
	fmt.Printf("%v\n", bytes)
	binary.BigEndian.PutUint32(bytes, uint32(buff.Len()-4))

	fmt.Printf("%v\n", bytes)

	conn.Write(bytes)

	fmt.Println("Sent Request")

	length := new(int32)
	err := binary.Read(conn, binary.BigEndian, length)
	if err != nil {
		fmt.Printf("Error reading from Kafka :%v \n", err)
		t.FailNow()
	}

	fmt.Println("Response OK")

	fmt.Println("stop")
}
