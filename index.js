var req    = require('superagent')
  , _      = require('lodash')
  , crypto = require('crypto')
;

module.exports = {
    /**
     *  Taken from https://github.com/mixpanel/mixpanel-node-export/blob/master/lib/mixpanel.js#L122
     */
    getUTC: function() {
        var d = new Date(),
            local_time = d.getTime(),
			// getTimezoneOffset returns diff in minutes (??? why?)
            local_offset = d.getTimezoneOffset() * 60 * 1000; 
        return local_time + local_offset;
    }

    /**
     *  Taken from https://github.com/mixpanel/mixpanel-node-export/blob/master/lib/mixpanel.js#L140
     */
    , signParams: function(params, api_secret) {
		// This signs unicode strings differently than the mixpanel backend
        var hash, key, keys, param, to_be_hashed, _i, _len;
        if (!(params !==  null ? params.api_key : void 0) || !(params !== null ? params.expire : void 0)) {
            throw new Error('all requests must have api_key and expire');
        }
        keys = Object.keys(params).sort();
        to_be_hashed = '';
        for (_i = 0, _len = keys.length; _i < _len; _i++) {
            key = keys[_i];
            if (key === 'callback' || key === 'sig') {
                continue;
            }
            param = {};
            param[key] = params[key];
            to_be_hashed += key + '=' + params[key];
        }
        hash = crypto.createHash('md5');
        hash.update(to_be_hashed + api_secret);
        params.sig = hash.digest('hex');
        return params;
    }

    /**
     *
     * The main entry point for the Dexter module
     *
     * @param {AppStep} step Accessor for the configuration for the step using this module.  Use step.input('{key}') to retrieve input data.
     * @param {AppData} dexter Container for all data used in this workflow.
     */
    , run: function(step, dexter) {
        this.total = 0;
        this.items = [];
        this.processResults(step);
    }

    /**
     *  Run the initial query and subsequent paged queries as necessary 
     *
     *  @param {AppStep} step
     */
    , processResults: function(step /*private*/, err, res) {
        var api_secret  = step.input('api_secret').first()
          , request_url = 'http://mixpanel.com/api/2.0/engage/?'
          , page        = _.get(data, 'page', -1)+1
          , expires     = Math.floor(this.getUTC() / 1000) + 600
          , params      = _.extend(
                            {expire: expires, page: page }
                            ,_.pick(step.inputs(), ['api_key', 'selector', 'behaviors'])
                          )
          , hash        = this.signParams(params, api_secret)
          , query       = _.reduce(params, function(q, val, key) { q.push([key, encodeURIComponent(val)].join('='));  return q; }, []).join('&')
          , data        = _.get(res, 'body')
          , self        = this
        ;

        query+=('&sig='+hash);


        if(err) return this.fail(err.stack);

        if(data) {
            _.each(data.results, function(result) {
                self.items.push(_.extend({$distinct_id: result.$distinct_id}, result.$properties));
            });
        }

        if(!data || data.results.length === 1000) {
            req.get(request_url+query)
              .type('json')
              .end(this.processResults.bind(this, step));
        } else {
            this.complete(this.items);
        }
    }
};
