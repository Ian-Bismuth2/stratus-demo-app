import React from 'react';
import {AsyncTypeahead} from 'react-bootstrap-typeahead';

import Api from './Api';

const LocationResult = ({loc}) => (
  <div>
    <span>{loc.name}</span>
  </div>
);

export default class LocationSearchField extends React.Component {
  state = {
    isLoading: false,
    options: [],
    error: null,
  };

  // Track component mounted state
  _isMounted = false;
  
  // For request cancellation
  _abortController = null;
  
  // For debouncing search
  _searchTimeout = null;

  componentDidMount() {
    this._isMounted = true;
  }

  componentWillUnmount() {
    this._isMounted = false;
    
    // Cancel any pending API requests
    if (this._abortController) {
      this._abortController.abort();
    }
    
    // Clear any pending timeouts
    if (this._searchTimeout) {
      clearTimeout(this._searchTimeout);
    }
  }

  render() {
    return (
      <AsyncTypeahead
        {...this.state}
        placeholder="Location"
        minLength={3}
        onSearch={this._handleSearch}
        labelKey="name"
        renderMenuItemChildren={(option, props) => (
          <LocationResult key={option.id} loc={option} />
        )}
        onChange={(selected) => {
            if (selected.length > 0) {
                this.props.onChange(selected);
            }
        }}
      />
    );
  }

  _handleSearch = (query) => {
    // Clear any existing timeout
    if (this._searchTimeout) {
      clearTimeout(this._searchTimeout);
    }

    // Cancel any pending request
    if (this._abortController) {
      this._abortController.abort();
    }

    // Create new abort controller for this request
    this._abortController = new AbortController();

    // Set loading state if component is still mounted
    if (this._isMounted) {
      this.setState({
        isLoading: true,
        error: null
      });
    }

    // Debounce the API call by 300ms
    this._searchTimeout = setTimeout(() => {
      Api.get("/location/search", {
        signal: this._abortController.signal,
        params: {
          q: query,
        }
      })
      .then(({data}) => {
        // Only update state if component is still mounted
        if (this._isMounted) {
          this.setState({
            isLoading: false,
            options: data,
            error: null
          });
        }
      })
      .catch(error => {
        // Don't update state if request was aborted or component unmounted
        if (error.name === 'AbortError' || !this._isMounted) {
          return;
        }
        
        // Handle other errors
        if (this._isMounted) {
          this.setState({
            isLoading: false,
            error: 'Failed to fetch locations. Please try again.',
            options: []
          });
        }
      });
    }, 300);
  }
}