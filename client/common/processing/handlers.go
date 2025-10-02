package common

import "fmt"

// TableRowHandler interface for handling any type of table row
type TableRowHandler interface {
	// ProcessRecord converts a CSV line into the protocol key-value map
	ProcessRecord(record []string) (map[string]string, error)
	// GetExpectedFields returns the number of columns the CSV should have
	GetExpectedFields() int
}

// MenuItemHandler handles menu_items table processing
type MenuItemHandler struct{}

func (m MenuItemHandler) ProcessRecord(record []string) (map[string]string, error) {
	if len(record) != 7 {
		return nil, fmt.Errorf("expected 7 fields, got %d: %v", len(record), record)
	}
	return map[string]string{
		"product_id":     record[0],
		"name":           record[1],
		"category":       record[2],
		"price":          record[3],
		"is_seasonal":    record[4],
		"available_from": record[5],
		"available_to":   record[6],
	}, nil
}

func (m MenuItemHandler) GetExpectedFields() int { return 7 }

// StoreHandler handles stores table processing
type StoreHandler struct{}

func (s StoreHandler) ProcessRecord(record []string) (map[string]string, error) {
	return map[string]string{
		"store_id":    record[0],
		"store_name":  record[1],
		"street":      record[2],
		"postal_code": record[3],
		"city":        record[4],
		"state":       record[5],
		"latitude":    record[6],
		"longitude":   record[7],
	}, nil
}

func (s StoreHandler) GetExpectedFields() int { return 8 }

// TransactionItemHandler handles transaction_items table processing
type TransactionItemHandler struct{}

func (t TransactionItemHandler) ProcessRecord(record []string) (map[string]string, error) {
	return map[string]string{
		"transaction_id": record[0],
		"item_id":        record[1],
		"quantity":       record[2],
		"unit_price":     record[3],
		"subtotal":       record[4],
		"created_at":     record[5],
	}, nil
}

func (t TransactionItemHandler) GetExpectedFields() int { return 6 }

// TransactionHandler handles transactions table processing
type TransactionHandler struct{}

func (t TransactionHandler) ProcessRecord(record []string) (map[string]string, error) {
	return map[string]string{
		"transaction_id":    record[0],
		"store_id":          record[1],
		"payment_method_id": record[2],
		"user_id":           record[3],
		"original_amount":   record[4],
		"discount_applied":  record[5],
		"final_amount":      record[6],
		"created_at":        record[7],
	}, nil
}

func (t TransactionHandler) GetExpectedFields() int { return 8 }

// UserHandler handles users table processing
type UserHandler struct{}

func (u UserHandler) ProcessRecord(record []string) (map[string]string, error) {
	return map[string]string{
		"user_id":       record[0],
		"gender":        record[1],
		"birthdate":     record[2],
		"registered_at": record[3],
	}, nil
}

func (u UserHandler) GetExpectedFields() int { return 4 }
