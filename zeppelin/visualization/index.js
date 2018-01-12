/**
 * Copyright 2017 Volume Integration
 * Copyright 2017 Tom Grant
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
import Visualization from 'zeppelin-vis'
import ColumnselectorTransformation from 'zeppelin-tabledata/columnselector'


import L from 'leaflet'
import 'leaflet/dist/leaflet.css'

// workaround https://github.com/Leaflet/Leaflet/issues/4968
import icon from 'leaflet/dist/images/marker-icon.png';
import iconShadow from 'leaflet/dist/images/marker-shadow.png';
let DefaultIcon = L.icon({
  iconUrl: icon,
  iconSize: [25, 41],
  iconAnchor: [12, 41],
  popupAnchor: [0, -41],
  tooltipAnchor: [12, -28],
  shadowUrl: iconShadow
});
L.Marker.prototype.options.icon = DefaultIcon;

import 'leaflet-polylinedecorator/dist/leaflet.polylineDecorator'

export default class LeafletMap extends Visualization {

  constructor(targetEl, config) {
    super(targetEl, config);

    const columnSpec = [
      { name: 'points'},
      { name: 'tooltip'},
      { name: 'points2'},
      { name: 'tooltip2'},
      { name: 'popup'}
    ];

    this.transformation = new ColumnselectorTransformation(config, columnSpec);
    this.chartInstance = L.map(this.getChartElementId());
  }

  getTransformation() {
    return this.transformation;
  }

  showChart() {
    super.setConfig(config);
    this.transformation.setConfig(config);
    if(!this.chartInstance) {
      this.chartInstance = L.map(this.getChartElementId());
    }
    return this.chartInstance;
  };

  getChartElementId() {
    return this.targetEl[0].id
  };

  getChartElement() {
    return document.getElementById(this.getChartElementId());
  };

  clearChart() {
    if (this.chartInstance) {
      this.chartInstance.off();
      this.chartInstance.remove();
      this.chartInstance = null;
    }
  };

  showError(error) {
    this.clearChart();
    this.getChartElement().innerHTML = `
        <div style="margin-top: 60px; text-align: center; font-weight: 100">
            <span style="font-size:30px;">
                ${error.message}
            </span>
        </div>`
  }

  getRandomColor() {
    var letters = '0123456789ABCDEF';
    var color = '#';
    for (var i = 0; i < 6; i++) {
      color += letters[Math.floor(Math.random() * 16)];
    }
    return color;
  }

  drawMapChart(chartDataModel) {

    const map = this.showChart();

    L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {
      attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
    }).addTo(map);

    var markers = chartDataModel.rows.map(
      row => {
        const {points, tooltip, points2, tooltip2, popup} = row;

        var marker = new L.Polyline(points, {color: this.getRandomColor(), weight: 4});
        var mapMarker = marker.addTo(map);

        if (tooltip && tooltip !== '') {
          mapMarker.bindTooltip(tooltip);
        }

        if (points2) {
          var marker2 = new L.Polyline(points2, {color: this.getRandomColor(), weight: 4});
          var mapMarker2 = marker.addTo(map);

          if (tooltip2 && tooltip2 !== '') {
            mapMarker2.bindTooltip(tooltip2);
          }
        }

        if (popup && popup !== '') {
          // mapMarker.bindPopup(popup);
          /*
          points.forEach(point => {
            var marker3 = new L.circle(point, {radius: 10});
            const mapMarker3 = marker3.addTo(map);
          });
          */
          var decorator = L.polylineDecorator(mapMarker, {
            patterns: [
                {offset: 0, repeat: 30, symbol: L.Symbol.arrowHead({pixelSize: 12, headAngle: 30, pathOptions: {weight: 3}})}
            ]
          }).addTo(map);
        }

        return marker
      }
    );

    let featureGroup = L.featureGroup(markers);
    const bounds = featureGroup.getBounds().pad(0.5);
    map.fitBounds(bounds);

    this.getChartElement().style.height = this.targetEl.height();
    map.invalidateSize(true);
  };

  createMapDataModel(data) {

    const getColumnIndex = (config, fieldName, isOptional) => {
      const fieldConf = config[fieldName];
      if(fieldConf instanceof Object) {
        return fieldConf.index
      } else if (isOptional) {
        return -1
      } else {
        throw {
          message: "Please set " + fieldName + " in Settings"
        }
      }
    };

    const config = this.getTransformation().config;
    const pointsIdx = getColumnIndex(config, 'points');
    const points2Idx = getColumnIndex(config, 'points2', true);
    const tooltipIdx = getColumnIndex(config, 'tooltip');
    const tooltip2Idx = getColumnIndex(config, 'tooltip2', true);
    const popupIdx = getColumnIndex(config, 'popup', true);

    const rows = data.rows.map(tableRow => {
      const tooltip = tableRow[tooltipIdx];
      const pointsStr = tableRow[pointsIdx];
      var re = /WrappedArray\(([\d\.]*), ([\d\.]*)\)/g;
      var m;
      var points = [];
      do {
        m = re.exec(pointsStr);
        if (m) {
          const lat = Number(m[1]);
          const lng = Number(m[2]);
          points.push(L.latLng(lat, lng));
        }
      } while (m);
      const points2 = points2Idx < 0 ? null : tableRow[points2Idx];
      const tooltip2 = tooltip2Idx < 0 ? null : tableRow[tooltip2Idx];
      const popup = popupIdx < 0 ? null : tableRow[popupIdx];
      return {
        points,
        tooltip,
        points2,
        tooltip2,
        popup
      };
    });

    return {
      rows
    };
  }

  render(data) {
    try {
      const mapDataModel = this.createMapDataModel(data);
      this.drawMapChart(mapDataModel)
    } catch (error) {
      console.error(error);
      this.showError(error)
    }
  }
}
