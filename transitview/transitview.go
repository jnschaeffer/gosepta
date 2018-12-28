// Package transitview contains functions and data for getting vehicle positions from SEPTA via
// the TransitView API.
package transitview

import (
	"encoding/json"
	"context"
	"net/http"
	"net/url"
	"io/ioutil"
	"fmt"
)

const (
	allPositionsURL = "https://www3.septa.org/hackathon/TransitViewAll/"
	routePositionsURL = "https://www3.septa.org/hackathon/TransitView/"
)

// VehiclePosition represents the position of a vehicle in the SEPTA fleet.
type VehiclePosition struct {
	Latitude float64 `json:"lat,string"`
	Longitude float64 `json:"lng,string"`
	Label string `json:"label"`
	VehicleID string `json:"VehicleID"`
	BlockID string `json:"BlockID"`
	Trip string `json:"trip"`
	Direction string `json:"Direction"`
	Destination string `json:"destination"`
	OffsetMinutes int `json:"offset,string"`
	OffsetSeconds int `json:"Offset_sec,string"`
	Heading int `json:"heading"`
	LateMinutes int `json:"late"`
}

// Client represents an HTTP client for fetching data from the SEPTA TransitView API.
type Client struct {
	httpClient http.Client

	allPositionsURL string
	routePositionsURL string
}

// NewClient creates a new Client.
func NewClient() *Client {
	return &Client{
		allPositionsURL: allPositionsURL,
		routePositionsURL: routePositionsURL,
	}
}

// AllVehiclePositions gets all vehicle positions across all SEPTA bus and trolley routes, stored
// as a map with route labels as keys and vehicle positions as values.
func (c *Client) AllVehiclePositions(ctx context.Context) (map[string][]VehiclePosition, error) {
	request, errRequest := http.NewRequest("GET", c.allPositionsURL, nil)
	if errRequest != nil {
		return nil, errRequest
	}
	request = request.WithContext(ctx)
	
	resp, errGet := c.httpClient.Do(request)
	if errGet != nil {
		return nil, errGet
	}

	body, errBody := ioutil.ReadAll(resp.Body)
	if errBody != nil {
		return nil, errBody
	}

	// This wonky data structure represents the actual response: a one-element list of
	// maps from string route labels to lists of vehicle positions
	var out struct {
		Routes []map[string][]VehiclePosition `json:"routes"`
	}

	errUnmarshal := json.Unmarshal(body, &out)
	if errUnmarshal != nil {
		return nil, errUnmarshal
	}

	return out.Routes[0], nil
}

// VehiclePositions gets all vehicle positions for a given SEPTA route.
func (c *Client) VehiclePositions(ctx context.Context, route string) ([]VehiclePosition, error) {
	routePath := url.PathEscape(route)
	routeURL := fmt.Sprintf("%s%s", c.routePositionsURL, routePath)

	request, errRequest := http.NewRequest("GET", routeURL, nil)
	if errRequest != nil {
		return nil, errRequest
	}
	request = request.WithContext(ctx)
	
	resp, errGet := c.httpClient.Do(request)
	if errGet != nil {
		return nil, errGet
	}

	body, errBody := ioutil.ReadAll(resp.Body)
	if errBody != nil {
		return nil, errBody
	}

	var out struct{
		Bus []VehiclePosition `json:"bus"`
	}

	errUnmarshal := json.Unmarshal(body, &out)
	if errUnmarshal != nil {
		return nil, errUnmarshal
	}

	return out.Bus, nil
}
