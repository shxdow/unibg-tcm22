package gare

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/beevik/etree"
	"github.com/guregu/dynamo"
	"github.com/tidwall/gjson"
)

const awsRegion string = "eu-central-1"
const bucketName string = "4745a-xmlresults"

type Req struct {
	XmlString string `json:"xml"`
	RaceId    uint64 `json:"id"`
}

type Resp struct {
	Message string `json:"message:"`
}

type race struct {
	RaceID string `dynamo:"raceId"`
}

// raceIdAvailability checks whether a race with a given ID exists already
// as of now, it allows multiple uploads to manage updates
func raceIdAvailability(id string) (bool, error) {

	var result race

	sess := session.Must(session.NewSession())
	db := dynamo.New(sess, &aws.Config{Region: aws.String(awsRegion)})
	table := db.Table("Races")

	err := table.Get("raceId", id).One(&result)
	log.Printf("debug: %v", err)
	if err != nil && err.Error() == "dynamo: no item found" {
		// name availabale
		result = race{RaceID: id}

		log.Printf("no race of id %s found", id)

		put_err := table.Put(result).Run()
		if put_err != nil {
			log.Printf("err in dynamo while putting item %v, %v", err, result)
			return false, err
		}

		log.Printf("dynamo put result %s", result.RaceID)

	} else if err != nil {
		log.Printf("err: %v, %v", err, result)
		return false, err
	}

	return true, nil
}

// checkXml() verifies whether the XML provided is syntactically correct
func checkXml(xml string) (bool, error) {
	doc := etree.NewDocument()
	err := doc.ReadFromString(xml)
	if err != nil {
		return false, err
	}
	return true, nil
}

func s3FileUpload(s3Client *s3.S3, bucket, xmlString string, raceId uint64) error {

	ok, err := checkXml(xmlString)
	if err != nil {
		return err
	}

	if !ok {
		return errors.New("xml malformed")
	}

	id := strconv.FormatUint(uint64(raceId), 10) + ".xml"

	_, err = raceIdAvailability(id)
	if err != nil {
		return err
	}

	out, err := s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(id),
		Body:   bytes.NewReader([]byte(xmlString)),
	})

	if err != nil {
		return err
	}

	fmt.Printf("s3 put object out: %v", out)

	return nil
}

func HandleRaceUploadRequest(req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {

	var r Req

	r.XmlString = gjson.Get(req.Body, "xml").String()
	r.RaceId = gjson.Get(req.Body, "id").Uint()

	if r.XmlString == "" {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusBadRequest,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       http.StatusText(http.StatusBadRequest),
		}, errors.New("missing parameter `xml`")
	}

	if r.RaceId == 0 {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusBadRequest,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       http.StatusText(http.StatusBadRequest),
		}, errors.New("missing parameter `int`")
	}

	session, err := session.NewSession(&aws.Config{Region: aws.String(awsRegion)})
	if err != nil {
		v := gjson.Get(err.Error(), "status")
		msg := fmt.Sprintf("could not initialize new aws session: %v", err)
		log.Println(msg)

		return events.APIGatewayProxyResponse{
			StatusCode: int(v.Int()),
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       http.StatusText(int(v.Int())),
		}, nil
	}

	xmlString := r.XmlString
	raceId := r.RaceId

	s3Client := s3.New(session)

	err = s3FileUpload(s3Client, bucketName, xmlString, raceId)
	if err != nil {
		v := gjson.Get(err.Error(), "status")
		msg := fmt.Sprintf("could not initialize new aws session: %v", err)
		log.Println(msg)
		return events.APIGatewayProxyResponse{
			StatusCode: int(v.Int()),
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       fmt.Sprintf("failed to upload to bucket: %v", err),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       fmt.Sprint("upload successful"),
	}, nil
}
