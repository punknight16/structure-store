var assert = require('assert');
var Shared = require('placeholder-shared')
var axios = require('axios')
axios.defaults.withCredentials=true

export function getAPIURL(path){
	return "/api/" + path

}
export function apiPostJson(path, queryParams, data,headers) {
	return apiPost(path, queryParams, data, "application/json")
}
export function apiPost(path, queryParams, data, contentType) {
	return apiMethod('post', path, queryParams, data, contentType)
}
export function apiMethod(method, path, queryParams, data, contentType) {
	var config = {
		'url':getAPIURL(path),
		'method':method,
		'headers': {'X-Requested-With': 'XMLHttpRequest',
			'Content-Type':contentType? contentType:"application/json", 
		},
		'crossDomain': true, 
		'params':queryParams,
		'data': data,
		'withCredentials':true,
	};
	return new Promise(function (resolve, reject)  {
		axios(config).then(function(response){
			let cookie = response.headers['set-cookie']
			console.log("set cookie to ", cookie)
			Shared.Utils.setCookie(cookie)
			resolve(response.data, cookie)
			}).catch(function(error){
				reject( error );
				})
		})
}


describe('Array', function() {
	describe('Register', function() {
		it('Registering account', function(done) {
			apiPostJson('register', {
				username:"a", 
				email:"a",
				password:"1",
				confirmPassword:"1",
			}).then((result, cookie) => {
				console.log("result", result)
					done()
			})
		})
		it('get state from server', function(done) {
			  this.timeout(30*1000);
			Shared.GetState.GetStateBlob().then((result) => {
				console.log("in done")
				done()
			})
		});
	});
});
