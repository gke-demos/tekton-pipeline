package fakecontrol

import (
	"encoding/json"
	"net/http"
)

func jsonUnmarshal(raw []byte, dst any) error {
	return json.Unmarshal(raw, dst)
}

func writeJSONResp(w http.ResponseWriter, code int, v any) {
	raw, _ := json.Marshal(v)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_, _ = w.Write(raw)
}
