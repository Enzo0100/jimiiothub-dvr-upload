package utils

import (
"encoding/json"
"net/http"
)

type JSONResponse struct {
Code    int         
Message string      
Data    interface{} 
}

func WriteJSON(w http.ResponseWriter, status int, resp JSONResponse) {
w.Header().Set("Content-Type", "application/json")
w.WriteHeader(status)
json.NewEncoder(w).Encode(resp)
}
