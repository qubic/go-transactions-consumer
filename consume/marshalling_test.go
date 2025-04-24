package consume

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

// IMPORTANT: the marshalling needs to be similar to the consumer code and the elastic index, otherwise
// the deserialization will fail and/or ingesting to elastic will not work.
func TestTransaction_MarshalAndUnmarshal(t *testing.T) {
	transaction := &Transaction{
		Hash:      "transaction-hash",
		Source:    "source-identity",
		Dest:      "destination-identity",
		Amount:    1,
		Tick:      2,
		InputType: 3,
		InputSize: 4,
		InputData: "input-data",
		Signature: "signature",
		Timestamp: 5,
		MoneyFlew: true,
	}

	expectedJson := `{"hash":"transaction-hash","source":"source-identity","destination":"destination-identity","amount":1,"tickNumber":2,"inputType":3,"inputSize":4,"inputData":"input-data","signature":"signature","timestamp":5,"moneyFlew":true}`
	marshalled, err := json.Marshal(transaction)
	assert.NoError(t, err)
	assert.Equal(t, expectedJson, string(marshalled))

	var unmarshalled Transaction
	err = json.Unmarshal(marshalled, &unmarshalled)
	assert.NoError(t, err)
	assert.Equal(t, transaction, &unmarshalled)

}

func TestTickTransactions_MarshalAndUnmarshal(t *testing.T) {
	tickTransactions := &TickTransactions{
		Epoch:      123,
		TickNumber: 456,
		Transactions: []Transaction{
			{Hash: "transaction-hash",
				Source:    "source-identity",
				Dest:      "destination-identity",
				Amount:    1,
				Tick:      2,
				InputType: 3,
				InputSize: 4,
				InputData: "input-data",
				Signature: "signature",
				Timestamp: 5,
				MoneyFlew: true,
			},
		},
	}

	expectedJson := `{"epoch":123,"tickNumber":456,"transactions":[{"hash":"transaction-hash","source":"source-identity","destination":"destination-identity","amount":1,"tickNumber":2,"inputType":3,"inputSize":4,"inputData":"input-data","signature":"signature","timestamp":5,"moneyFlew":true}]}`
	marshalled, err := json.Marshal(tickTransactions)
	assert.NoError(t, err)
	assert.Equal(t, expectedJson, string(marshalled))

	var unmarshalled TickTransactions
	err = json.Unmarshal(marshalled, &unmarshalled)
	assert.NoError(t, err)
	assert.Equal(t, tickTransactions, &unmarshalled)

}
