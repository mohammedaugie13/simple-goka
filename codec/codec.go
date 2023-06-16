package codec

import (
	"google.golang.org/protobuf/proto"
	"simple_goka/event"
	"simple_goka/model"
)

type Codec struct{}

func (c *Codec) Encode(value interface{}) ([]byte, error) {

	pbDeposit := &model.Deposit{
		WalletId:  value.(event.Event).WalletID,
		Amount:    value.(event.Event).Amount,
		Timestamp: value.(event.Event).Timestamp,
	}
	buff, err := proto.Marshal(pbDeposit)
	return buff, err

}

func (c *Codec) Decode(data []byte) (interface{}, error) {
	event := new(event.Event)
	msg := &model.Deposit{}
	err := proto.Unmarshal(data, msg)
	event.WalletID = msg.WalletId
	event.Timestamp = msg.Timestamp
	event.Amount = msg.Amount
	return event, err
}
