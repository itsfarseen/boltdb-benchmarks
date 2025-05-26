package strategy

// An example struct that has many fields
type UserInfo struct {
	ID          int64   `json:"id"`
	Username    string  `json:"username"`
	Email       string  `json:"email"`
	FirstName   string  `json:"first_name"`
	LastName    string  `json:"last_name"`
	Age         int32   `json:"age"`
	Height      float32 `json:"height"`
	Weight      float32 `json:"weight"`
	Balance     float64 `json:"balance"`
	IsActive    bool    `json:"is_active"`
	CreatedAt   int64   `json:"created_at"`
	UpdatedAt   int64   `json:"updated_at"`
	LoginCount  int32   `json:"login_count"`
	Score       float64 `json:"score"`
	Description string  `json:"description"`
}
