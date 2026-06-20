package web

import "net/http"

type DeviceView struct {
	ID   int64
	Name string
}

type TokenView struct {
	Token      string
	Created    string
	ValidUntil string
}

type DevicesPage struct {
	CSRFToken string
	Devices   []DeviceView
}

type DeviceListView struct {
	Devices []DeviceView
}

type DeviceEditView struct {
	Device DeviceView
	Tokens []TokenView
}

func RenderDevicesPage(w http.ResponseWriter, p DevicesPage) error {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	return templates.ExecuteTemplate(w, "devices", p)
}

func RenderDeviceList(w http.ResponseWriter, v DeviceListView) error {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	return templates.ExecuteTemplate(w, "device-list", v)
}

func RenderDeviceEdit(w http.ResponseWriter, v DeviceEditView) error {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	return templates.ExecuteTemplate(w, "device-edit", v)
}
