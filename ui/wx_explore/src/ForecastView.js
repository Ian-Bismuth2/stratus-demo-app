import React from 'react';
import moment from 'moment';
import {Line as LineChart} from 'react-chartjs-2';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Spinner from 'react-bootstrap/Spinner';

import Api from './Api';

const lineColors = {
  'hrrr': '255,0,0',
  'gfs':  '0,255,0',
  'nam':  '0,0,255',
};

const metricsToDisplay = [
  "1", // temperature
  "3", // rain
  "6", // snow
  "12", // wind
  "15", // cloud cover
];

function capitalize(s) {
  return s[0].toUpperCase() + s.substring(1)
}

export default class ForecastView extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      location: null,
      metrics: null,
      sources: null,
      source_fields: null,
      summary: null,
      wx: null,
      loading: false,
      error: null,
    };
    
    // Initialize abort controllers for API requests
    this.abortControllers = {
      sources: null,
      metrics: null,
      location: null,
      wx: null,
      summary: null,
    };
  }

  getWx() {
    // Cancel any existing weather requests
    if (this.abortControllers.wx) this.abortControllers.wx.abort();
    if (this.abortControllers.summary) this.abortControllers.summary.abort();
    
    // Create new abort controllers
    this.abortControllers.wx = new AbortController();
    this.abortControllers.summary = new AbortController();
    
    let t = Math.round((new Date()).getTime() / 1000);
    
    this.setState({ loading: true, error: null });
    
    // Use Promise.all to coordinate concurrent requests
    Promise.all([
      Api.get("/wx", {
        signal: this.abortControllers.wx.signal,
        params: {
          lat: this.state.location.lat,
          lon: this.state.location.lon,
          start: t,
          end: t + (3 * 24 * 60 * 60), // 3 days out
        },
      }),
      Api.get("/wx/summarize", {
        signal: this.abortControllers.summary.signal,
        params: {
          lat: this.state.location.lat,
          lon: this.state.location.lon,
          days: 1,
        },
      })
    ]).then(([wxResponse, summaryResponse]) => {
      this.setState({
        wx: wxResponse.data,
        summary: summaryResponse.data,
        loading: false
      });
    }).catch(error => {
      // Don't update state if request was aborted
      if (error.name === 'AbortError') return;
      
      this.setState({
        error: 'Failed to fetch weather data. Please try again.',
        loading: false
      });
    });
  }

  componentDidMount() {
    this.setState({ loading: true, error: null });

    // Create new abort controllers for initial requests
    this.abortControllers.sources = new AbortController();
    this.abortControllers.metrics = new AbortController();
    this.abortControllers.location = new AbortController();

    // Load sources and metrics data
    Promise.all([
      Api.get("/sources", { signal: this.abortControllers.sources.signal }),
      Api.get("/metrics", { signal: this.abortControllers.metrics.signal })
    ]).then(([sourcesResponse, metricsResponse]) => {
      let sources = {};
      let source_fields = {};
      for (let src of sourcesResponse.data) {
        sources[src.id] = src;
        for (let field of src.fields) {
          source_fields[field.id] = field;
        }
      }

      let metrics = {};
      for (let metric of metricsResponse.data) {
        metrics[metric.id] = metric;
      }

      this.setState({ sources, source_fields, metrics, loading: false });
    }).catch(error => {
      if (error.name === 'AbortError') return;
      this.setState({
        error: 'Failed to load initial data. Please refresh the page.',
        loading: false
      });
    });
    
    // Load location data if provided
    if (this.props.match.params.loc_id !== undefined) {
      Api.get(`/location/${this.props.match.params.loc_id}`, {
        signal: this.abortControllers.location.signal
      }).then(({data}) => {
        this.setState({location: data});
      }).catch(error => {
        if (error.name === 'AbortError') return;
        this.setState({
          error: 'Failed to load location data.',
          loading: false
        });
      });
    } else if (this.props.match.params.lat !== undefined && this.props.match.params.lon !== undefined) {
      Api.get("/location/by_coords", {
        signal: this.abortControllers.location.signal,
        params: {
          lat: this.props.match.params.lat,
          lon: this.props.match.params.lon,
        },
      }).then(({data}) => {
        this.setState({
          location: {
            lat: this.props.match.params.lat,
            lon: this.props.match.params.lon,
            name: `Near ${data.name}`,
          }
        });
      }).catch(error => {
        if (error.name === 'AbortError') return;
        this.setState({
          error: 'Failed to load location data.',
          loading: false
        });
      });
    }
  }

  componentDidUpdate(prevProps, prevState) {
    // only attempt to fetch when we have a location...
    if (this.state.location == null) {
      return;
    }
    
    // ... or when location changed
    if (prevState.location === this.state.location) {
      return;
    }

    this.setState({wx: null, summary: null});
    this.getWx();
  }

  componentWillUnmount() {
    // Cancel all pending API requests
    Object.values(this.abortControllers).forEach(controller => {
      if (controller) {
        controller.abort();
      }
    });
  }

  chartjsData() {
    let metrics = {}; // map[metric_id, map[source_id, map[run_time, list]]] 

    for (const ts of this.state.wx.ordered_times) {
      for (const data_point of this.state.wx.data[ts]) {
        const source_field = this.state.source_fields[data_point.src_field_id]
        const metric = this.state.metrics[source_field.metric_id];
        const source = this.state.sources[source_field.source_id];

        if (!(metric.id in metrics)) {
          metrics[metric.id] = {};
        }

        if (!(source.id in metrics[metric.id])) {
          metrics[metric.id][source.id] = {};
        }

        if (!(data_point.run_time in metrics[metric.id][source.id])) {
          metrics[metric.id][source.id][data_point.run_time] = [];
        }

        const [val, ] = this.props.converter.convert(data_point.value, metric.units);
        metrics[metric.id][source.id][data_point.run_time].push({x: new Date(ts * 1000), y: val});
      }
    }

    let datasets = {};
    for (const metric_id in metrics) {
      if (!metricsToDisplay.includes(metric_id)) {
        continue;
      }

      datasets[metric_id] = [];

      for (const source_id in metrics[metric_id]) {
        const source = this.state.sources[source_id];

        let earliest_run = 0;
        let latest_run = 0;
        for (const run_time in metrics[metric_id][source_id]) {
          if (earliest_run === 0 || run_time < earliest_run) {
            earliest_run = run_time;
          } else if (run_time > latest_run) {
            latest_run = run_time;
          }
        }

        for (const run_time in metrics[metric_id][source_id]) {
          let alpha = 0.15;
          if (run_time === latest_run) {
            alpha = 0.8;
          }

          const run_name = moment.unix(run_time).utc().format("HH[Z] dddd Do") + " " + source.name;
          const color = 'rgba('+lineColors[source.short_name]+','+alpha+')';

          datasets[metric_id].push({
            label: run_name,
            data: metrics[metric_id][source_id][run_time],
            fill: false,
            backgroundColor: color,
            borderColor: color,
            pointBorderColor: color,
          });
        }
      }
    }

    return datasets;
  }

  coreMetricsBox(day) {
    const summary = this.state.summary[day];

    let cloudCoverIcon = '';
    switch (summary.cloud_cover[0].cover) {
      case 'clear':
        cloudCoverIcon = 'wi-day-sunny';
        break;
      case 'mostly clear':
        cloudCoverIcon = 'wi-day-sunny';
        break;
      case 'partly cloudy':
        cloudCoverIcon = 'wi-day-cloudy-high';
        break;
      case 'mostly cloudy':
        cloudCoverIcon = 'wi-cloud';
        break;
      case 'cloudy':
        cloudCoverIcon = 'wi-cloudy';
        break;
      default:
        cloudCoverIcon = 'wi-alien'; // idk
    }

    return (
      <Row className="justify-content-md-center">
        <Col md={2}>
          <i style={{fontSize: "7em"}} className={"wi " + cloudCoverIcon}></i>
        </Col>
        <Col md={3}>
          <h4>{this.props.converter.convert(summary.temps[0].temperature, 'K')} {capitalize(summary.cloud_cover[0].cover)}</h4>
          <p>High: {this.props.converter.convert(summary.high.temperature, 'K')}</p>
          <p>Low: {this.props.converter.convert(summary.low.temperature, 'K')}</p>
        </Col>
      </Row>
    );
  }

  summarize(day) {
    let components = [];

    for (const [index, component] of this.state.summary[day].summary.components.entries()) {
      let text = '';
      if (index === 0) {
        text = capitalize(component.text);
      } else {
        text = component.text;
      }
      text += ' ';

      if (component.type === 'text') {
        components.push(<span key={index}>{text}</span>);
      } else {
        components.push(<span key={index}>{text}</span>);
      }
    }

    return (
      <span>{components}</span>
    );
  }

  render() {
    if (this.state.summary == null || this.state.sources == null || this.state.source_fields == null || this.state.metrics == null) {
      return (
        <Spinner animation="border" role="status">
          <span className="sr-only">Loading...</span>
        </Spinner>
      );
    }

    let charts = [];

    if (this.state.wx == null) {
      charts.push(
        <Spinner animation="border" role="status">
          <span className="sr-only">Loading...</span>
        </Spinner>
      );
    } else {
      let datasets = this.chartjsData();
      const options = {
        scales: {
          xAxes: [{
            type: 'time',
            distribution: 'linear',
            time: {
              unit: 'hour',
            },
            ticks: {
              min: moment(),
              max: moment().add(3, 'days'),
            },
          }],
        },
        legend: {
          display: false,
        },
      };

      for (const metric_id in datasets) {
        const metric = this.state.metrics[metric_id];
        const data = {
          datasets: datasets[metric_id],
        };
        let opts = {
          ...options,
          title: {
            display: true,
            text: metric.name,
          },
        };
        charts.push(
          <Row>
            <Col>
              <LineChart key={metric.name} data={data} options={opts}/>
            </Col>
          </Row>
        );
      };
    }

    return (
      <div>
        <Row className="justify-content-md-center">
          <Col md="auto">
            <h2>{this.state.location.name}</h2>
          </Col>
        </Row>
        {this.coreMetricsBox(0)}
        <Row className="justify-content-md-center">
          <Col md="auto">
            <p style={{fontSize: "1.5em"}}>{this.summarize(0)}</p>
          </Col>
        </Row>
        <hr/>
        {charts}
      </div>
    );
  }
}