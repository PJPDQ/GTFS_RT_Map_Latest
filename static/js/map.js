let mymap = L.map('mapid').setView([-27.469770, 153.025131], 11);
///GLOBAL VARIABLES
//NORMAL TILE LAYER
// let osm = L.tileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
//     attribution: '&copy; <a href="http://openstreetmap.org/copyright">OpenStreetMap</a> contributors'
//   }).addTo(mymap);
//DARK MODE
var osm = L.tileLayer('https://tiles.stadiamaps.com/tiles/alidade_smooth_dark/{z}/{x}/{y}{r}.png',{
  attribution: '&copy; <a href="https://stadiamaps.com/">Stadia Maps</a>, &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors'
}).addTo(mymap);
var baseMaps = {
  "OpenStreetMap": osm
}
let geoData_state, pointLayers;
let stopMap, cMarker;
let layerControl = L.control.layers(baseMaps, null, {collapsed: true});
var focused_trip = null, sliderControlEnable = false;
var path, sliderControl = null;
function bearing(coord1, coord2) {
  let theta = Math.atan2(coord2[1] - coord1[1], coord2[0] - coord1[0]);
  theta *= 180 / Math.PI
  return theta;
}
//VEHICLES POSITIONS
let buses = {};
let schedules = {};
function updateCircleMarkerOptions(marker, newCoords) {
  var latlng = marker.getLatLng();
  var popup = marker.getPopup();
  var existingOpts = marker.options.iconOptions;
  mymap.removeLayer(marker);
  marker = L.marker.arrowCircle(latlng, {
    ...marker.options,
    iconOptions: Object.assign({}, existingOpts, {rotation: bearing(latlng, newCoords)}),
  })
  .bindPopup(popup)
  .addTo(mymap);
  return marker;
}

function onMapClick(e) {
  Object.keys(buses).filter((key) => key !== focused_trip).map((unkey) => buses[unkey].markers[0].setOpacity(1));
  liveFilter(HFBs);
  if (path) {
    mymap.removeLayer(path);
  }
  if (sliderControlEnable) {
    mymap.removeControl(sliderControl);
    sliderControlEnable = false;
  };
  focused_trip = null;
}
mymap.on('click', onMapClick);
/////////////////
// PT Icons Settings
var options = (obj, delay, rotation) => {
  obj = {...obj, "delay": delay}
  if (obj.trip_id.includes("QR")) {
    return L.marker([obj.latitude, obj.longitude], {
      title: 'Train TripID ' + obj.trip_id,
      clickable: true,
      draggable: false,
      icon: trainIcon,
      alt: obj
    }).bindPopup(popUpContent(obj), {offset:[-23, -35]});
  } else if (obj.trip_id.includes("BCC")) {
    return L.marker([obj.latitude, obj.longitude], {
      title: 'Ferry TripID ' + obj.trip_id,
      clickable: true,
      draggable: false,
      icon: ferryIcon,
      alt: obj
    }).bindPopup(popUpContent(obj), {offset:[-21, -30]}); 
  } else {
    if (delay < 150) { // late less than 2.5mins
      return circleMarkerText([obj.latitude, obj.longitude], obj, 0.25, 0.25, 'circle1', rotation).bindPopup(popUpContent(obj), {offset:[-12, 10]});
    } else if (delay > 150 && delay < 300) {
      return circleMarkerText([obj.latitude, obj.longitude], obj, 0.25, 0.25, 'circle1mid', rotation).bindPopup(popUpContent(obj), {offset:[-12, 10]});
    } else {
      return circleMarkerText([obj.latitude, obj.longitude], obj, 0.25, 0.25, 'circle1late', rotation).bindPopup(popUpContent(obj), {offset:[-12, 10]});
    }
  }
};

// Miscellaneous 
function removeMarkersTrip(newMarkers, mymap) {
  for (var i = newMarkers.length-1; i > -1; i--) {
    mymap.removeLayer(newMarkers[i]);
  }
}
var route_splitter = (obj) => {
  return obj.route_id.split("-")[0];
}
function markers_sender() {
  if (focused_trip) {
    let markers = buses[focused_trip];
    let temp_list = [];
    for (var i = 0; i < markers.length; i++) {
      let obj = markers[i].options.alt
      temp_list.push(obj);
    }
  }
}
function liveFilter(selectedBuses) {
  let routes = [];
  if (!Array.isArray(selectedBuses)) {
    routes = [selectedBuses];
  } else {
    routes = selectedBuses;
  }
  request_data = JSON.stringify({'routes': routes})
  $.ajax({
    url: '/',
    type: 'POST',
    contentType: "application/json; charset=utf-8",
    data: request_data,
    dataSrc: "checkedboxRoutes",
    error: function(e) {
      console.log(e);
    },
    success: function(response) {
      mymap.removeLayer(geoData_state);
      if (response == "") {
        layerControl.clearLayers();
      } else {
        makeStateMap(response);
      }
    },
  });
}
//////////////////////////
/////CHECKBOXES
//Static Checkboxes Front-End Generator
function render_checkboxes_routes() {
  let myRoutes = document.getElementById("checkboxRoutes");
  for (let i = 0; i < HFBs.length; i++) {
    let checkBox = document.createElement("input");
    let label = document.createElement("label");
    let br = document.createElement("br");
    checkBox.type = "checkbox";
    checkBox.value = HFBs[i];
    checkBox.name = "routes";
    myRoutes.appendChild(checkBox);
    myRoutes.appendChild(label);
    label.appendChild(document.createTextNode(HFBs[i]));
    myRoutes.appendChild(br);
  }
}
//Changes occurring on the Checkboxes Function
const checkedBoxRoutes = document.getElementById("checkboxRoutes");
let selectedRoutes = [];
// Update Layers based on these changes
function makeStateMap(response) {
  traj_shp = response["traj_shp"];
  stop_shp = response["stop_shp"];
  let state = JSON.parse(traj_shp);
  let stop = JSON.parse(stop_shp);
  stopMap.clearLayers();
  layerControl.removeLayer(geoData_state);
  layerControl.removeLayer(stopMap);
  geoData_state = new L.geoJson(state, {
    style: newStyle,
    pointToLayer: handlePoint,
    onEachFeature: handleFeature,
  }).addTo(mymap);

  stopMap = new L.geoJson(stop, {
    style: handleStyle,
    pointToLayer: handlePoint,
    onEachFeature: handleFeature
  }).addTo(mymap);
  layerControl.addOverlay(geoData_state, "geojson_data");
  layerControl.addOverlay(stopMap, "stop_data");
  // mymap.addLayer(layerControl);
}
function toggle(source) {
  let checkboxes = document.getElementsByName("routes");
  for (let i = 0; i < checkboxes.length; i++) {
    checkboxes[i].checked = source.checked;
  }
}

//PT Vehicles Position Popup Template
var popUpContent = (obj) => {
  var d = new Date(obj.timestamp_dt);
  var delay_min = (obj.delay / 60).toFixed(1);
  var text_delay;
  if (delay_min < 1 || delay_min > -1) {
    text_delay = `${delay_min} min`
  } else {
    text_delay = `${delay_min} mins`
  }
  return '<p name="trip-id" id="trip-id" class="clearable">' + obj.trip_id + '</p>\
      <table class="popup-table" id="table-vals">\
      <tr class="popup-table-row">\
      <th class="popup-table-header">Datetime:</th>\
      <td name="time-dt" id="time-dt" class="popup-table-data">' + d.toLocaleString() + '</td>\
      </tr>\
      <tr class="popup-table-row">\
      <th class="popup-table-header">Timestamp:</th>\
      <td name="route-id" id="route-id" class="popup-table-data">' + obj.timestamp + '</td>\
      </tr>\
      <tr class="popup-table-row">\
      <th class="popup-table-header">Route ID:</th>\
      <td name="route-id" id="route-id" class="popup-table-data">' + obj.route_id + '</td>\
      </tr>\
      <tr class="popup-table-row">\
      <th class="popup-table-header">Stop ID:</th>\
      <td name="route-id" id="route-id" class="popup-table-data">' + obj.stop_id + '</td>\
      </tr>\
      <th class="popup-table-header">Arrival Delay:</th>\
      <td name="route-id" id="route-id" class="popup-table-data">' + text_delay + '</td>\
      </tr>\
  </table>'
}
//Circle Marker with Text
function circleMarkerText(latLng, obj, radius, borderWidth, circleClass, rotation) {
  let icon = changeIcon(obj, radius, borderWidth, circleClass, rotation);
  let route_id = route_splitter(obj);
  var marker = L.marker.arrowCircle(latLng, {
    iconOptions: icon,
    title: 'Bus ' + route_id,
    clickable: true,
    draggable: false,
    alt: obj,
  });
  if (focused_trip !== null && obj.trip_id !== focused_trip) {
    marker.setOpacity(0.35);
  }
  marker.on('click', markerOnClick);
  return(marker);
}

function changeIcon(obj, radius, borderWidth, circleClass, rotation) {
  var size = radius * 2;
  var style = 'style="width: ' + size + 'em; height: ' + size + 'em; border-width: ' + borderWidth + 'em;"';
  var iconSize = size + (borderWidth * 150);
  var route = route_splitter(obj);
  var text = '<span class="' + 'circle ' + circleClass + '" ' + style + '>' + route + '</span>';
  let color;
  if (circleClass == 'circle1') {
    color = "greenyellow"
  } else if (circleClass == 'circle1mid') {
    color = "orange"
  } else {
    color = "red"
  }
  return {
    color: color,
    size: iconSize,
    html: text,
    rotation: rotation, 
    route_id: route,
    obj_detail: obj
  }
}

var markerOnClick = (e) => {
  console.log(e.sourceTarget.options);
  let obj = e.sourceTarget.options.alt;
  var route = route_splitter(obj);
  focused_trip = obj.trip_id;
  Object.keys(buses).filter((key) => key !== obj.trip_id).map((unkey) => buses[unkey].markers[0].setOpacity(0.35));
  if (HFBs.includes(route)) {
    liveFilter(route);
    // markers_sender();
  }
}
//////////////////////////////////////////
///////////STATIC ASSETS (e.g., Stops and Path) Fetcher
function render_trajectory(response) {
  traj_shp = response["traj_shp"];
  stop_shp = response["stop_shp"];
  var states = JSON.parse(traj_shp);
  var stops = JSON.parse(stop_shp);
  properties = []
  geoData_state = new L.geoJson(states, {
      style: handleStyle,
      pointToLayer: handlePoint,
      onEachFeature: handleFeature,
  }).addTo(mymap);

  layerControl.addOverlay(geoData_state, "geojson_data");

  stopMap = new L.geoJson(stops, {
    style: handleStyle,
    pointToLayer: handlePoint,
    onEachFeature: handleFeature,
  }).addTo(mymap);
  layerControl.addOverlay(stopMap, "stop_data");
  layerControl.addTo(mymap);
  if (states.features.length > 0) {
      var props = []
      states.features.forEach(x => props.push(x.properties.time));
  }
}
//////////////////////////
//////PT Vehicles Fetcher
var source = new EventSource('/gtfs_data'); //ENTER YOUR TOPICNAME HERE
source.addEventListener('message', function(e){
  obj = JSON.parse(e.data);
  data_length = Object.keys(obj).length;
  if (data_length == 16) {
    schedules = obj;
    let trips = Object.values(obj.trip_id);
    let vehicles = Array.from(Object.keys(buses));
    let remove_vehs = vehicles.filter(vehicle => !trips.includes(vehicle));
    remove_vehs.forEach(r_veh => {
      try{
        if (!HFBs.includes(route_splitter(buses[r_veh].markers[0].options.alt))) {
          removeMarkersTrip(buses[r_veh].markers, mymap);
          delete buses[r_veh];
        } else {
          var temp = buses[r_veh].markers.map((opt) => opt.options.alt.stop_id);
          temp = [... new Set(temp)];
          var last_stop_id = buses[r_veh].stop_updated;
          if (buses[r_veh].updated && temp[0] === last_stop_id) {
            removeMarkersTrip(buses[r_veh].markers, mymap);
            delete buses[r_veh];
          }
        }
      } catch(err) {
        console.log(r_veh);
        console.log(buses[r_veh].markers[0].options.alt);
      }
    });
    let delays = Object.values(obj.arrival_delay);
    let stop_ids = Object.values(obj.stop_id);
    Object.keys(buses).forEach((trip) => {
      if (trips.includes(trip)) {
        buses[trip]['delay'] = delays[trips.indexOf(trip)];
        buses[trip]['stop_updated'] = stop_ids[trips.indexOf(trip)];
      }
    });
  } else {
    if (obj.latitude !== null && obj.longitude !== null) {
      if (!(obj.trip_id in buses)) {
        let markers = [];
        let marker1 = options(obj, 0, 0).addTo(mymap);
        markers.push(marker1);
        buses[obj.trip_id] = {"markers": markers, "updated": true};
      } else {
        let newMarkers = buses[obj.trip_id].markers;
        let updated_flag = buses[obj.trip_id].updated;
        let delay = 0;
        if ("delay" in buses[obj.trip_id]) delay = buses[obj.trip_id].delay;
        let marker_lats = newMarkers.map((marker) => marker.options.alt.latitude);
        let marker_longs = newMarkers.map((marker) => marker.options.alt.longitude);
        if (marker_lats.indexOf(parseFloat(obj.latitude)) == -1 && marker_longs.indexOf(parseFloat(obj.longitude)) == -1) {
          let rot = bearing(buses[obj.trip_id].markers[0].getLatLng(), [obj.latitude, obj.longitude]);
          let marker1 = options(obj, delay, rot);
          newMarkers.push(marker1);
          let route_id = route_splitter(obj);
          if (HFBs.indexOf(route_id) !== -1) {
            if (newMarkers.length < 3) {
              buses[obj.trip_id].markers[0] = updateCircleMarkerOptions(buses[obj.trip_id].markers[0], [obj.latitude, obj.longitude]);
              buses[obj.trip_id].markers = newMarkers;
            } else { //length >= 3  
              if (!updated_flag) {
                buses[obj.trip_id].markers = newMarkers;
              } else { //true iff the Object is updating or have updated
                removeMarkersTrip(newMarkers, mymap);
                let firstMarker = newMarkers[0];
                var temp_list = newMarkers.map((marker) => marker.options.alt);
                buses[obj.trip_id].markers = [newMarkers.pop()];
                buses[obj.trip_id].updated = false;
                request_data = JSON.stringify({'markers': temp_list});
                $.ajax({url: '/', type: 'POST', contentType: "application/json; charset=utf-8",
                  data: request_data,
                  dataSrc: "slideBuses",
                  error: function(e) {
                    if (firstMarker) mymap.removeLayer(firstMarker);
                    buses[obj.trip_id].updated = true;
                  },
                  success: function(response) {
                    if (firstMarker) mymap.removeLayer(firstMarker);
                    let jsonRes = JSON.parse(response.features);
                    let trip_id = jsonRes.features[0].properties.trip_id;
                    if ("speed_sms" in jsonRes.features[0].properties && trip_id in buses)  {
                      let inter_feat = jsonRes.features;
                      let tempMs = [firstMarker];
                      let number_interpolated = inter_feat.length;
                      let interval = ((1800 / number_interpolated) > 3000) ? 3000 : (1800 / number_interpolated);
                      let i = 1;
                      var loop = function () {
                        return new Promise(function (outerResolve) {
                          var promise = Promise.resolve();
                          var next = function () {
                            var tempMark = inter_feat.shift();
                            if (firstMarker) {
                              mymap.removeLayer(firstMarker);
                            }
                            if (firstMarker.getLatLng().lat != tempMark.geometry.coordinates[1] || firstMarker.getLatLng().long != tempMark.geometry.coordinates[0]) {
                              // var prev = firstMarker.getLatLng();
                              var rotating = bearing(firstMarker.getLatLng(), [tempMark.geometry.coordinates[1], tempMark.geometry.coordinates[0]])
                              firstMarker = circleMarkerText(
                                [tempMark.geometry.coordinates[1], tempMark.geometry.coordinates[0]], 
                                tempMark.properties, 0.25, 0.25, 'circle1', rotating);
                              firstMarker.addTo(mymap).bindPopup(pointPopUp(tempMark.properties));
                              tempMs.push(tempMark);
                            }
                            if (++i < number_interpolated) {
                              promise = promise.then(function () {
                                return new Promise(function (resolve) {
                                  setTimeout(function () {
                                    resolve();
                                    next();
                                  }, interval);
                                });
                              });
                            } else {
                              if (firstMarker) mymap.removeLayer(firstMarker);
                              outerResolve();
                            }
                          };
                          next();
                        });
                      };
                      loop().then(function () {
                        try{
                          var new_first = buses[trip_id].markers[0];
                          new_first.addTo(mymap);
                          buses[trip_id].updated = true;
                        } catch (err) {
                          console.log(err);
                          console.log(trip_id);
                        }
                      });
                    } else {
                      if(firstMarker) mymap.removeLayer(firstMarker);
                      buses[trip_id].updated = true;
                    }
                  },
                });
              }
            }
          } else {
            removeMarkersTrip(newMarkers, mymap);
            marker1.addTo(mymap);
            if (newMarkers.length < 3 && !(obj.trip_id.includes("QR") | obj.trip_id.includes("BCC"))) buses[obj.trip_id].markers[0] = updateCircleMarkerOptions(buses[obj.trip_id].markers[0], [obj.latitude, obj.longitude]);
            if (newMarkers.length >= 3) newMarkers.shift();
            buses[obj.trip_id].markers = newMarkers;
            buses[obj.trip_id].updated = true;
          }
        }
      }
    }
  }
}, false);
//////////////////////////