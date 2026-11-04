package booking

import (
	pb "github.com/hhoshii/flight-booking-homework-3/gen/flight/v1"
	"time"
)

type flightView struct {
	ID             string    `json:"id"`
	FlightNumber   string    `json:"flight_number"`
	Airline        string    `json:"airline"`
	Origin         string    `json:"origin"`
	Destination    string    `json:"destination"`
	DepartureTime  time.Time `json:"departure_time"`
	ArrivalTime    time.Time `json:"arrival_time"`
	TotalSeats     int32     `json:"total_seats"`
	AvailableSeats int32     `json:"available_seats"`
	Price          int64     `json:"price"`
	Status         string    `json:"status"`
}

func viewFlight(f *pb.Flight) flightView {
	return flightView{ID: f.Id, FlightNumber: f.FlightNumber, Airline: f.Airline, Origin: f.Origin, Destination: f.Destination, DepartureTime: f.DepartureTime.AsTime(), ArrivalTime: f.ArrivalTime.AsTime(), TotalSeats: f.TotalSeats, AvailableSeats: f.AvailableSeats, Price: f.Price, Status: f.Status.String()}
}
