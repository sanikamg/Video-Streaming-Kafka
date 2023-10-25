package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"gocv.io/x/gocv"
)

var (
	kafkaBroker = "localhost:9092"
	topic       = "SampleVideo"
)

func main() {
	r := gin.Default()

	r.LoadHTMLGlob("template/*")
	r.Static("/static", "static/")
	r.GET("/", serveHTML)

	r.GET("/kafka-stream", streamKafka)

	log.Println("Server is running on :8080")
	log.Fatal(r.Run(":8080"))
}

func serveHTML(c *gin.Context) {
	c.HTML(http.StatusOK, "index.html", gin.H{})
}

func streamKafka(c *gin.Context) {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer([]string{kafkaBroker}, config)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatal(err)
	}
	defer partitionConsumer.Close()

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")

	var frameBuffer []gocv.Mat

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			frameData, err := gocv.IMDecode(msg.Value, gocv.IMReadColor)
			if err != nil {
				log.Println("Error decoding frame:", err)
				return
			}

			frame := frameData.Clone()

			frameBuffer = append(frameBuffer, frame)

			// If enough frames accumulated, encode and send video segment
			if len(frameBuffer) >= 50 {
				// Encode frames into video format
				videoData, err := encodeFramesToVideo(frameBuffer)
				if err != nil {
					log.Println("Error decoding frame:", err)
					return
				}

				// Send video segment to client
				sendVideoSegmentToClient(videoData, c)

				// Clear frame buffer
				frameBuffer = nil
			}
		}
	}
}

func encodeFramesToVideo(frames []gocv.Mat) ([]byte, error) {
	// Create a VideoWriter to encode the frames into a video format
	writer, err := gocv.VideoWriterFile("output.mp4", "avc1", 30, frames[0].Cols(), frames[0].Rows(), true)
	if err != nil {
		return nil, err
	}
	defer writer.Close()

	// Write each frame to the video writer
	for _, frame := range frames {
		if err := writer.Write(frame); err != nil {
			return nil, errors.New("failed to write frame to video")
		}
	}

	// Read the encoded video file into memory
	videoData, err := ioutil.ReadFile("output.mp4")
	if err != nil {
		return nil, err
	}

	return videoData, nil
}

// func sendVideoSegmentToClient(videoData []byte, c *gin.Context) {
// 	// Set the appropriate headers for serving video content
// 	c.Header("Content-Type", "video/mp4")
// 	c.Status(http.StatusOK)

// 	// Write the video data to the response writer
// 	_, err := c.Writer.Write(videoData)
// 	if err != nil {
// 		log.Println("Error sending video segment:", err)
// 	}
// }

// func sendVideoSegmentToClient(videoData []byte, c *gin.Context) {
// 	// Set the appropriate headers for serving video content
// 	c.Header("Content-Type", "video/mp4")
// 	c.Header("Content-Length", strconv.Itoa(len(videoData)))

// 	// Use ServeContent to send the video data
// 	http.ServeContent(c.Writer, c.Request, "output.mp4", time.Now(), bytes.NewReader(videoData))

// }

func sendVideoSegmentToClient(videoData []byte, c *gin.Context) {
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")

	chunkSize := 1024 // Adjust this based on your desired chunk size

	for i := 0; i < len(videoData); i += chunkSize {
		end := i + chunkSize
		if end > len(videoData) {
			end = len(videoData)
		}

		chunk := videoData[i:end]

		fmt.Fprintf(c.Writer, "data: %x\n\n", chunk)
		c.Writer.Flush()
		time.Sleep(100 * time.Millisecond) // Adjust this for desired chunking rate
	}
}
