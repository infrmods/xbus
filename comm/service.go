package comm

type ServiceEndpoint struct {
	Type    string
	Address string
	Config  string `json:",omitempty"`
	Proto   string `json:",omitempty"`
}
