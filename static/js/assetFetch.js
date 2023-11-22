let HFBs = ['60', '61', '100', '111' ,'120', '130' ,'140', '150', '180', '196' , '199' ,'200' ,'222', '330', '333' ,'340', '345', '385', '412', '444', '555'];

setInterval( function () {
  var dt = new Date();
  document.getElementById('date-time').innerHTML=dt;
}, 1);

var busIcon = L.icon({
    iconUrl: '/static/assets/aesthetic/bus.png',
    iconSize: [15, 15],
    iconAnchor: [10, 41],
});
var ferryIcon = L.icon({
  iconUrl: '/static/assets/aesthatic/ferry.png',
  iconSize: [15, 15],
  iconAnchor: [10, 41],
});
var trainIcon = L.icon({
  iconUrl: '/static/assets/aesthatic/train.png',
  iconSize: [15, 15],
  iconAnchor: [10, 41],
});

// Static Assets (e.g., Line and Stops) Setting Configuration
function handleStyle(feature) {
    if (feature.geometry.type == "LineString") {
      return {
          weight: 3,
          color: 'green',
          opacity: 0.7,
      }
    } else if (feature.geometry.type == "Point") {
      return {
          fillColor: 'white',
          weight: 2,
          color: 'black',
          opacity: 1,
      }
    }
  }
  function newStyle(feature) {
    return {
        // fillColor: setRegionColor(feature.properties.time),
        weight: 3,
        color: 'green',
        opacity: 0.7,
        // fillOpacity: setRegionOpacity(feature.properties.time)
    }
  }

  function isString(str) {
    return /^[a-zA-Z()]+$/.test(str);
  }

  function handlePoint(feature, latlng) {
    var pointOptions = {
      radius: 3.5,
      weight: 1,
      opacity: 1,
      fillOpacity: 1
    }
    let popup = popupTemplate(feature.properties);
    cMarker = new L.circleMarker(latlng, pointOptions).bindPopup(popup).openPopup();
    cMarker.setStyle({color: 'red', fillColor: 'red'});
    return cMarker;
  }
  function handleFeature(feature, layer) {
    if (feature.properties) {
        prop = feature.properties;
        // layer.on('click', layerClickHandler);
        if (feature.geometry.type == 'Polygon') {
            layer.bindPopup(
                // NEED TO BE FINISHED!!!
                featureTemplate(prop)
            )
        } 
    }
  }
  // Static Asset Template
  function popupTemplate(prop) {
    return '<table class="popup-table" id="table-vals">\
        <tr class="popup-table-row">\
        <th class="popup-table-header">RouteID:</th>\
        <td name="route_id" id="route_id" class="popup-table-data">' + prop["Route id"] + '</td>\
        </tr>\
        <tr class="popup-table-row">\
        <th class="popup-table-header">StopID:</th>\
        <td name="stop_id" id="stop_id" class="popup-table-data">' + prop["Stop Id"] + '</td>\
        </tr>\
        <tr class="popup-table-row">\
        <th class="popup-table-header">Stop Name:</th>\
        <td name="stop_name" id="stop_name" class="popup-table-data">' + prop["Stop Name"] + '</td>\
        </tr>\
        <tr class="popup-table-row">\
        <th class="popup-table-header">Stop Latitude:</th>\
        <td name="stop_lat" id="stop_lat" class="popup-table-data">' + prop["Stop Lat"] + '</td>\
        </tr>\
        <tr class="popup-table-row">\
        <th class="popup-table-header">Stop Longitude:</th>\
        <td name="stop_lon" id="stop_lon" class="popup-table-data">' + prop["Stop Lon"] + '</td>\
        </tr>\
    </table>'
  }

  function pointPopUp(prop) {
    var d = new Date(prop.actual_timestamp);
    return '<table class="popup-table" id="interpolated_vals">\
        <tr class="popup-table-row">\
        <th class="popup-table-header">RouteID:</th>\
        <td name="route_id" id="route_id" class="popup-table-data">' + prop["route_id"] + '</td>\
        </tr>\
        <tr class="popup-table-row">\
        <th class="popup-table-header">StopID:</th>\
        <td name="stop_id" id="stop_id" class="popup-table-data">' + prop["stop_id"] + '</td>\
        </tr>\
        <tr class="popup-table-row">\
        <th class="popup-table-header">TripID:</th>\
        <td name="stop_name" id="stop_name" class="popup-table-data">' + prop["trip_id"] + '</td>\
        </tr>\
        <tr class="popup-table-row">\
        <th class="popup-table-header">Point Latitude:</th>\
        <td name="stop_lat" id="stop_lat" class="popup-table-data">' + prop["latitude"] + '</td>\
        </tr>\
        <tr class="popup-table-row">\
        <th class="popup-table-header">Point Longitude:</th>\
        <td name="stop_lon" id="stop_lon" class="popup-table-data">' + prop["longitude"] + '</td>\
        </tr>\
        <tr class="popup-table-row">\
        <th class="popup-table-header">Datetime:</th>\
        <td name="stop_lon" id="stop_lon" class="popup-table-data">' + d.toLocaleString() + '</td>\
        </tr>\
        <tr class="popup-table-row">\
        <th class="popup-table-header">Distance Travelled (km):</th>\
        <td name="stop_lon" id="stop_lon" class="popup-table-data">' + prop["distance_travelled_km"] + '</td>\
        </tr>\
        <tr class="popup-table-row">\
        <th class="popup-table-header">Travel Time (hour):</th>\
        <td name="stop_lon" id="stop_lon" class="popup-table-data">' + parseFloat(prop["travel_time_sec"]) + ' secs</td>\
        </tr>\
        <tr class="popup-table-row">\
        <th class="popup-table-header">Speed (km/h):</th>\
        <td name="stop_lon" id="stop_lon" class="popup-table-data">' + prop["speed_sms"] + '</td>\
        </tr>\
    </table>'
  }