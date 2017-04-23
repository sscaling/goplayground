package kafka

import (
	"testing"
	"net"
	"fmt"
	"time"
	"bytes"
	"encoding/binary"
	"reflect"
	"hash/crc32"
	"io"
)

type subject struct {
	i64 int64
	i32 int32
	i16 int16
	s   [5]byte
}


// Testing binary.Write size investigation
func TestStructSize(testing *testing.T) {
	s := subject{int64(64), int32(32), int16(16), [5]byte{'h','e','l','l','o'}}

	v := reflect.Indirect(reflect.ValueOf(s))
	var t reflect.Type = v.Type()
	fmt.Println(t)



	switch t.Kind() {

	case reflect.Struct:
		sum := 0
		fmt.Printf("NumField ? %v\n", t.NumField())
		for i, n := 0, t.NumField(); i < n; i++ {
			fmt.Printf("%d type %v, size %d\n", i, t.Field(i).Type, binary.Size(t.Field(i).Type))
			//s := sizeof(t.Field(i).Type)
			//if s < 0 {
			//	return -1
			//}
			//sum += s
		}
		fmt.Println(sum)
	}

}

func TestSliceSize(t *testing.T) {

	//b := []byte{1,2,3,4}
	b := make([]byte, 4)
	fmt.Println(binary.Size(b))
}

func TestEncodingStructAsByteSlice(t *testing.T) {

	s := subject{int64(64), int32(32), int16(16), [5]byte{'h','e','l','l','o'}}


	fmt.Printf("%v\n", binary.Size([]byte{1}))


	//// *bytes.Buffer type
	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.BigEndian, s)
	if err != nil {
		t.Errorf("Failed to encode struct : %v\n", err)
		t.FailNow()
	}

	fmt.Printf("% x\n", buff.Bytes())

	expected := []byte{
		0, 0, 0, 0, 0, 0, 0, 64,
		0, 0, 0, 32,
		0, 16,
		'h', 'e', 'l', 'l', 'o',
	}

	if len(buff.Bytes()) != len(expected) {
		t.Errorf("not expected size, expected %d, actual %d\n", len(expected), len(buff.Bytes()))
		t.FailNow()
	}

	for i, v := range(buff.Bytes()) {
		if v != expected[i] {
			t.FailNow()
		}
	}
}

/**
The protocol is built out of the following primitive types.

Fixed Width Primitives

int8, int16, int32, int64 - Signed integers with the given precision (in bits) stored in big endian order.

Variable Length Primitives

bytes, string - These types consist of a signed integer giving a length N followed by N bytes of content.
A length of -1 indicates null. string uses an int16 for its size, and bytes uses an int32.

Arrays

This is a notation for handling repeated structures. These will always be encoded as an int32 size containing
the length N followed by N repetitions of the structure which can itself be made up of other primitive types.
In the BNF grammars below we will show an array of a structure foo as [foo].
 */

func TestKafka(t *testing.T) {
	conn, err := net.Dial("tcp", "kafka:9092")
	if err != nil {
		fmt.Printf("Cannot establish connection to kafka broker : %v\n", err)
		t.FailNow()
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(5 * time.Second))

	fmt.Println("Connected")

	// Send Versions request

	header := new(bytes.Buffer)

	//
	// Headers
	// apiKey int16        : 0 - 20 API key, the function of this request
	// apiVersion int16    : version of API
	// correlationId int32 : user supplied id, that will be passed back to the client
	// clientIdLen int16   : Length of client Id (-1 is null)
	// clientId []byte     : clientId data, may be omitted
	writeBigEndian(header, int16(ApiVersions))
	writeBigEndian(header, int16(0))
	writeBigEndian(header, int32(13))
	writeBigEndian(header, int16(-1)) // null client Id

	//
	// Message
	// Crc int32          : CRC32 of the remainder of the message bytes
	// MagicByte int8     : Always 1
	// Attributes int8    : bits(xxxxyzzz), x always 0, y timestamp type (0 create, 1 append time), z compression codec
	// Timestamp int64    : ms since unix epoch UTC
	// keyLen int16       : length of key (-1 means null, with no 'key' value specified)
	// key []byte         : key data
	// value []byte       : opaque byte slice. Kafka supports recursive messages
	content := new(bytes.Buffer)
	writeBigEndian(content, int8(1))
	writeBigEndian(content, int8(0))
	writeBigEndian(content, time.Now().Unix())
	writeBigEndian(content, int32(-1)) // no key

	//
	// Value
	// Note: this is changed per request type
	// example ApiVersions (key: 18)
	writeBigEndian(content, int32(-1)) // no payload for this API request

	// need to build message first, to calculate CRC32
	crc := crc32.ChecksumIEEE(content.Bytes())
	fmt.Printf("Calculated CRC32 : %v\n", crc)
	// FIXME: any issue with uint32 -> int32 ?
	msg := new(bytes.Buffer)
	writeBigEndian(msg, int32(crc))
	// Then write message
	content.WriteTo(msg)

	fmt.Printf("Message ? %v\n", msg.Bytes())

	//
	// MessageSet
	// Offset int64       : -1 can be used when uncompressed data is being sent
	// MessageSize int32  : the size of the subsequent message
	// Message
	messageSet := new(bytes.Buffer)
	writeBigEndian(messageSet, int64(1))
	writeBigEndian(messageSet, int32(len(msg.Bytes())))
	msg.WriteTo(messageSet)

	fmt.Printf("messageSet : %v\n", messageSet.Bytes())

	request := new(bytes.Buffer)
	header.WriteTo(request)
	messageSet.WriteTo(request)

	writeBigEndian(conn, int32(len(request.Bytes())))
	conn.Write(request.Bytes())

	fmt.Println("Sent Request");

	length := new(int32)
	err = binary.Read(conn, binary.BigEndian, length)
	if err != nil {
		fmt.Printf("Error reading from Kafka :%v \n", err)
		t.FailNow()
	}

	fmt.Println("Response OK")

	fmt.Printf("Response length : %d\n", *length)
	response := make([]byte, *length)
	i, err := conn.Read(response)
	if err != nil || i != int(*length) {
		fmt.Printf("Couldn't read response")
		t.FailNow()
	}

	fmt.Printf("Response : %v\n", response)
	r := bytes.NewBuffer(response)

	// response header only contains correlationId
	correlationId := new(int32)
	readBigEndian(r, correlationId)
	fmt.Printf("CorrelationId : %d\n", *correlationId)


	errorCode := new(int16)
	readBigEndian(r, errorCode)
	fmt.Printf("Error code : %d\n", *errorCode)
	if ErrNone != int(*errorCode) {
		t.FailNow()
	}

	fmt.Printf("Message : %v\n", response[6:])

	// now read [api_versions]
	// api_versions => api_key min_version max_version
	//	api_key => INT16
	//	min_version => INT16
	//	max_version => INT16
	// .. it's an array, so get size

	type Version struct {
		ApiKey int16
		MinVersion int16
		MaxVersion int16
	}

	arrSize := new(int32)
	readBigEndian(r, arrSize)
	fmt.Printf("Next value is %d bytes\n", *arrSize)

	for i := 0; i < int(*arrSize); i += 1 {
		fmt.Printf("i : %d\n", i)

		v := new(Version)
		readBigEndian(r, v)
		fmt.Printf("Version : key %d, min %d, max %d\n", v.ApiKey, v.MinVersion, v.MaxVersion)
	}

	if _, err := r.ReadByte(); err != io.EOF {
		t.Errorf("Data still to be consumed")
	} else {
		fmt.Println(err)
	}
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