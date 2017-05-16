package kafka

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"os"
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

func connect() net.Conn {
	conn, err := net.Dial("tcp", "kafka:9092")
	if err != nil {
		fmt.Printf("Cannot establish connection to kafka broker : %v\n", err)
		//		t.FailNow()
	}

	conn.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Println("Connected")

	return conn
}

// Response

type responseHeader struct {
	size int32
}

type response interface {
	Decode(io.Reader) (response, error)
}

type apiVersion struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

type versionsResponse struct {
	responseHeader
	errorCode int16
	versions  []apiVersion
}

func (r *versionsResponse) Decode(reader io.Reader) (response, error) {
	fmt.Printf("%v\n", *r)
	readBigEndian(reader, &r.size)
	readBigEndian(reader, &r.errorCode)

	if ErrNone == r.errorCode {
		readArray(reader, func(reader io.Reader) {
			v := new(apiVersion)
			readBigEndian(reader, v)
			r.versions = append(r.versions, *v)
		})
	} else {
		return nil, errors.New(fmt.Sprintf("Received error %v\n", r.errorCode))
	}

	fmt.Printf("%v\n", *r)
	return r, nil
}

func TestKafkaRead() {
	fmt.Println("start")

	conn := connect()
	defer conn.Close()

	r := requestMessage{
		requestHeader{
			correlationId: 15,
			//apiKey:        ApiMetaData,
			//apiVersion:    ApiVersionOne,
			apiKey:     ApiVersions,
			apiVersion: ApiVersionZero,
		},
		kafkaString(nil),
		[]requestBody{
			NewRequestBody(nil, VersionsRequest{}.Encode()),
			//NewRequestBody(nil, MetaDataRequest{"topiclogs"}.Encode()),
		},
	}

	err := sendMessage(conn, &r)

	length := new(int32)
	/*	err = binary.Read(conn, binary.BigEndian, length)
		if err != nil {
			fmt.Printf("Error reading from Kafka :%v \n", err)
		}
	*/
	readBigEndian(conn, length)

	fmt.Println("Response OK")

	// now we need to process the response
	// first int32 is the message size, allocate that amount of data, and read response
	fmt.Printf("Response length : %d\n", *length)
	response := make([]byte, *length)
	i, err := conn.Read(response)
	if err != nil || i != int(*length) {
		fmt.Printf("Couldn't read response")
	}
	// create a NewBuffer for io.Reader interface, no allocations used
	fmt.Printf("Response : %v\n", response)
	buff := bytes.NewBuffer(response)

	var resp versionsResponse
	rx, err := resp.Decode(buff)
	if err != nil {
		fmt.Printf("Failed to decode: %v\n", err)
		os.Exit(2)
	}

	fmt.Printf("%v\n", rx)

	if _, err := buff.ReadByte(); err != io.EOF {
		fmt.Errorf("Data still to be consumed")
	} else {
		fmt.Println(err)
	}

	fmt.Println("stop")
}

func sendMessage(conn net.Conn, r *requestMessage) error {
	buff := new(bytes.Buffer)
	r.Encode(buff)

	fmt.Printf("len ? %d\n", buff.Len())

	bytes := buff.Bytes()
	binary.BigEndian.PutUint32(bytes, uint32(buff.Len()-4))

	count, err := conn.Write(bytes)
	if err != nil {
		fmt.Printf("Sent %d bytes", count)
	}

	return err
}

func writeBigEndian(w io.Writer, data interface{}) {
	if err := binary.Write(w, binary.BigEndian, data); err != nil {
		fmt.Printf("Couldn't write : %v\n", data)
	}
}

func readBigEndian(r io.Reader, data interface{}) {
	if err := binary.Read(r, binary.BigEndian, data); err != nil {
		fmt.Printf("Couldn't read : %v\n", data)
	}
}

func readArray(r io.Reader, f func(io.Reader)) {
	arrSize := new(int32)
	readBigEndian(r, arrSize)
	fmt.Printf("Array has %d elements\n", *arrSize)

	for i := 0; i < int(*arrSize); i++ {
		f(r)
	}

	// FIXME: Errors?
}
