package main

import (
	"log"

	"github.com/IBM/sarama"
	"gocv.io/x/gocv"
)

var (
	kafkaBroker = "localhost:9092"
	topic       = "SampleVideo"
)

func main() {
	// Create a Kafka producer

	producer, err := sarama.NewSyncProducer([]string{kafkaBroker}, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	// Initialize a video capture device (assuming it's properly set up in your environment)
	capture, err := initializeVideoCapture("sample.mp4")
	if err != nil {
		log.Fatal(err)
	}
	defer capture.Close()

	frame := sarama.ByteEncoder{} // Initialize an empty ByteEncoder

	for {
		// Read a frame from the video capture
		frameData, ok := readFrame(capture)
		if !ok {
			break
		}

		// Set the frame data
		frame = sarama.ByteEncoder(frameData)

		// Send the frame as a message
		message := &sarama.ProducerMessage{
			Topic: topic,
			Value: frame,
		}

		// Send the message
		_, _, err = producer.SendMessage(message)
		if err != nil {
			log.Println(err)
		}
	}
}

//	func initializeVideoCapture() (*gocv.VideoCapture, error) {
//		// Open the system camera (usually 0 is the default camera)
//		capture, err := gocv.OpenVideoCapture(0)
//		return capture, err
//	}
func initializeVideoCapture(filePath string) (*gocv.VideoCapture, error) {
	// Open the video file
	capture, err := gocv.VideoCaptureFile(filePath)
	return capture, err
}

func readFrame(capture *gocv.VideoCapture) ([]byte, bool) {
	frame := gocv.NewMat()
	defer frame.Close()

	if ok := capture.Read(&frame); !ok {
		return nil, false
	}

	// Convert frame to JPEG byte slice
	img, err := gocv.IMEncode(gocv.JPEGFileExt, frame)
	if err != nil {
		log.Println(err) // Handle the error gracefully (log it for now)
		return nil, false
	}

	return img.GetBytes(), true
}
