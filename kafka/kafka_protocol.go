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

func decodeString(r io.Reader) (string, error) {
	l := new(int16)
	readBigEndian(r, l)

	if *l != int16(-1) {
		// a non-null string
		buff := make([]byte, *l)
		readBigEndian(r, buff)
		// FIXME: errors?
		fmt.Printf("Read string '%s'\n", string(buff))

		return string(buff), nil
	} else {
		fmt.Println("null string")
	}

	return "", nil
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
	// size is read before decoding message
	//size          int32
	corellationId int32
}

func (r *responseHeader) Decode(reader io.Reader) (responseHeader, error) {
	//readBigEndian(reader, &r.size)
	readBigEndian(reader, &r.corellationId)
	// FIXME: errors
	return *r, nil
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
	h, _ := r.responseHeader.Decode(reader)
	r.responseHeader = h
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

	return r, nil
}

type metadataResponse struct {
	responseHeader
	brokers []metadataBrokers
	//controllerId int32
	metadata []topicMetadata
}

type metadataBrokers struct {
	nodeId int32
	host   string
	port   int32
	//rack   string
}

// FIXME: How to nicely fix the errors in this situation?
func decodeMetadataBrokers(r io.Reader) (m metadataBrokers, err error) {
	readBigEndian(r, &m.nodeId)
	m.host, err = decodeString(r)
	readBigEndian(r, &m.port)
	//m.rack, err = decodeString(r)
	return
}

type partitionMetadata struct {
	errorCode   int16
	partitionId int32
	leader      int32
	replicas    []int32
	isr         []int32
}

func (p *partitionMetadata) Decode(reader io.Reader) (partitionMetadata, error) {
	readBigEndian(reader, p.errorCode)
	readBigEndian(reader, p.partitionId)
	readBigEndian(reader, p.leader)

	readArray(reader, func(reader io.Reader) {
		i := new(int32)
		readBigEndian(reader, i)
		p.replicas = append(p.replicas, *i)
	})

	readArray(reader, func(reader io.Reader) {
		i := new(int32)
		readBigEndian(reader, i)
		p.isr = append(p.isr, *i)
	})

	return *p, nil
}

type topicMetadata struct {
	errorCode int16
	topic     string
	//isInternal bool
	partitions []partitionMetadata
}

func readBool(r io.Reader) bool {
	b := new(int8)
	readBigEndian(r, b)
	return *b == 1
}

func (r *topicMetadata) Decode(reader io.Reader) (topicMetadata, error) {
	fmt.Printf("topicMetadata.Decode")

	readBigEndian(reader, r.errorCode)
	s, err := decodeString(reader)
	if err == nil {
		r.topic = s
	}
	//r.isInternal = readBool(reader)

	fmt.Printf("%v\n", *r)
	readArray(reader, func(reader io.Reader) {
		var p partitionMetadata
		result, _ := p.Decode(reader)
		r.partitions = append(r.partitions, result)
	})

	fmt.Printf("%v\n", *r)
	return *r, nil
}

func (r *metadataResponse) Decode(reader io.Reader) (response, error) {
	fmt.Printf("metadataResponse.Decode %v\n", *r)

	h, _ := r.responseHeader.Decode(reader)
	r.responseHeader = h

	fmt.Printf("2: %v\n", *r)
	// [brokers] controller_id [topic_metadata]

	readArray(reader, func(reader io.Reader) {
		b, err := decodeMetadataBrokers(reader)
		if err == nil {
			r.brokers = append(r.brokers, b)
		} else {
			fmt.Println("Can't decode [brokers]")
			// FIXME: propagate error
		}
	})

	// version 1 only
	// readBigEndian(reader, r.controllerId)
	fmt.Println("Processing metadata")

	readArray(reader, func(reader io.Reader) {
		var t topicMetadata
		metadata, _ := t.Decode(reader)
		r.metadata = append(r.metadata, metadata)
	})
	fmt.Printf("%v\n", *r)

	return r, nil
}

func TestKafkaRead() {
	fmt.Println("start")

	conn := connect()
	defer conn.Close()
	/*
		r := requestMessage{
			requestHeader{
				correlationId: 15,
				apiKey:        ApiVersions,
				apiVersion:    ApiVersionZero,
			},
			kafkaString(nil),
			[]requestBody{
				NewRequestBody(nil, VersionsRequest{}.Encode()),
			},
		}
	*/

	// meta-data request
	r := requestMessage{
		requestHeader{
			correlationId: 17,
			apiKey:        ApiMetaData,
			apiVersion:    ApiVersionZero,
		},
		kafkaString(nil),
		[]requestBody{
			NewRequestBody(nil, MetaDataRequest{"test"}.Encode()),
		},
	}

	err := sendMessage(conn, &r)

	buff, err := bufferResponse(conn)
	if err != nil {
		fmt.Errorf("Unable to buffer response %v\n", err)
		os.Exit(2)
	}

	fmt.Println("Decoding response")

	//	var resp versionsResponse
	var resp metadataResponse
	rx, err := resp.Decode(buff)
	if err != nil {
		fmt.Printf("Failed to decode: %v\n", err)
		os.Exit(2)
	}

	fmt.Printf("%v\n", rx)

	var notused []byte
	if _, err := buff.Read(notused); err != io.EOF {
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
		fmt.Printf("Couldn't read : %v, %v\n", data, err)
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

func bufferResponse(conn net.Conn) (io.Reader, error) {
	length := new(int32)
	readBigEndian(conn, length)

	// now we need to process the response
	// first int32 is the message size, allocate that amount of data, and read response
	fmt.Printf("Response length : %d\n", *length)
	response := make([]byte, *length)
	i, err := conn.Read(response)
	if err != nil || i != int(*length) {
		return nil, errors.New("Couldn't read response")
	}

	// create a NewBuffer for io.Reader interface, no allocations used
	fmt.Printf("Response : %v\n", response)
	return bytes.NewBuffer(response), nil
}
