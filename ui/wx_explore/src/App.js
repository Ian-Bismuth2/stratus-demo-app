import React from "react";
import Nav from "react-bootstrap/Nav";
import Navbar from "react-bootstrap/Navbar";
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Form from "react-bootstrap/Form";
import Spinner from "react-bootstrap/Spinner";
import { Switch, Route, withRouter } from "react-router-dom";

import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faGithub } from "@fortawesome/free-brands-svg-icons";

import ForecastView from "./ForecastView";
import LocationSearchField from "./LocationSearch";
import { Imperial, Metric } from "./Units";

import "./App.css";

class App extends React.Component {
  state = {
    location: null,
    unitConverter: null,
  };

  componentDidMount() {
    // Determine if user is in a metric region based on locale
    // US uses imperial, most other countries use metric
    const userLocale = navigator.language || navigator.userLanguage || '';
    const isMetricRegion = !userLocale.startsWith('en-US');
    
    this.setState({
      unitConverter: isMetricRegion ? new Metric() : new Imperial()
    });
  }

  render() {
    if (this.props.location.pathname === "/" && navigator.geolocation) {
      navigator.geolocation.getCurrentPosition((position) => {
        const lat = position.coords.latitude;
        const lon = position.coords.longitude;

        this.props.history.push(`/coords/${lat}/${lon}`);
      });
    }

    const year = new Date().getFullYear();

    return (
      <div className="App">
        <Navbar bg="dark" variant="dark">
          <Navbar.Brand>Stratus - Demo App</Navbar.Brand>
          <Nav className="mr-auto"></Nav>

          <Form inline>
            <LocationSearchField
              onChange={(selected) => {
                const { id } = selected[0];
                this.props.history.push(`/id/${id}`);
              }}
            />
          </Form>
        </Navbar>

        <Container style={{ marginTop: "1em" }}>
          <Switch>
            <Route
              path={`/id/:loc_id`}
              component={(props) => (
                this.state.unitConverter ? 
                <ForecastView converter={this.state.unitConverter} {...props} /> :
                <Spinner animation="border" role="status">
                  <span className="sr-only">Loading...</span>
                </Spinner>
              )}
            />
            <Route
              path={`/coords/:lat/:lon`}
              component={(props) => (
                this.state.unitConverter ? 
                <ForecastView converter={this.state.unitConverter} {...props} /> :
                <Spinner animation="border" role="status">
                  <span className="sr-only">Loading...</span>
                </Spinner>
              )}
            />

            <Route path="/">
              <Row className="justify-content-md-center">
                <Col md="auto">
                  <h4>Enter your location to get started</h4>
                </Col>
              </Row>
            </Route>
          </Switch>
        </Container>

        <footer className="footer">
          <Container fluid={true} style={{ height: "100%" }}>
            <Row className="h-100">
              <Col xs="4" className="align-self-center">
                <a
                  target="_blank"
                  rel="noopener noreferrer"
                  href="https://github.com/kallsyms/wx_explore"
                  className="text-muted"
                >
                  kallsyms/wx_explore on{" "}
                  <FontAwesomeIcon icon={faGithub} size="lg" />
                </a>
                <br />
                <span className="text-muted">&copy; {year}</span>
              </Col>

              <Col xs="8" className="align-self-center">
                <span
                  className="text-muted"
                  style={{
                    fontSize: "0.75em",
                    display: "block",
                    lineHeight: "1.5em",
                  }}
                >
                  The data on this website is best-effort, and no guarantees are
                  made about the availability or correctness of the data. It
                  should not be used for critical decision making.
                </span>
              </Col>
            </Row>
          </Container>
        </footer>
      </div>
    );
  }
}

export default withRouter(App);