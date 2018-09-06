var i = 0;
var countryCode = "SG"; //MODIFY THIS WITH THE COUNTRY CODE THAT YOU ARE UPLOADING
var counter = 0;
var totalCounter = 0;
var lineCount = 0;
var wait = 0;
var fs = require('fs')
    , util = require('util')
    , stream = require('stream')
    , es = require('event-stream');

var lineNr = 0;
var counter = 0;
var counterGlobal = 0;
var mysql = require('mysql');
function handleDisconnect() {
  connection = mysql.createConnection({
		  host     : 'localhost',
		  user     : 'root',
		  password : ''
		}); // Recreate the connection, since
                                                  // the old one cannot be reused.

  connection.connect(function(err) {              // The server is either down
    if(err) {                                     // or restarting (takes a while sometimes).
      console.log('error when connecting to db:', err);
      setTimeout(handleDisconnect, 2000); // We introduce a delay before attempting to reconnect,
    }                                     // to avoid a hot loop, and to allow our node script to
  });                                     // process asynchronous requests in the meantime.
                                          // If you're also serving http, display a 503 error.
  connection.on('error', function(err) {
    console.log('db error', err);
    if(err.code === 'PROTOCOL_CONNECTION_LOST') { // Connection to the MySQL server is usually
      handleDisconnect();                         // lost due to either server restart, or a
    }
    else if(err.code === 'PROTOCOL_ENQUEUE_AFTER_FATAL_ERROR')
    {
    	handleDisconnect();
    }
    else {                                      // connnection idle timeout (the wait_timeout
      throw err;                                  // server variable configures this)
    }
  });
}

handleDisconnect();
//MODIFY NEXT LINE WITH FILENAME F THE FILE YOU WANT TO UPLOAD TO MYSQL
var s = fs.createReadStream('/var/www/html/ds_dump_SG_2.jl') 
    .pipe(es.split())
    .pipe(es.mapSync(function(line){
        // pause the readstream
        s.pause();
        var profile = JSON.parse(line);
        insertToMysql(profile,function(response){
            s.resume();
        });
        
        // resume the readstream, possibly from a callback

    })
    .on('error', function(err){
		console.log(err);
        console.log('Error while reading file.');
    })
    .on('end', function(){
        console.log('Read entire file.');
    })
);
function insertToMysql(profile,callback)
{
	if(typeof profile.experience !== 'undefined' && typeof profile.experience[0].organization !== 'undefined' && profile.experience[0].organization[0].unique_id !== 'undefined') //PROCESS THE PROFILE BECAUSE HAVE COMPANY
	{
		var first_name = (typeof profile.given_name !== 'undefined') ? profile.given_name.split(",").join(" ") : "-";
		var last_name = (typeof profile.family_name !== 'undefined') ? profile.family_name.split(",").join(" ") : "-";
		var fullname = (typeof profile.full_name !== 'undefined') ? profile.full_name.split(",").join(" ") : "-";
		var profile_url = (typeof profile.canonical_url !== 'undefined') ? profile.canonical_url.split(",").join(" ") : "-";
		var connections = (typeof profile.num_connections !== 'undefined') ? profile.num_connections.split(",").join(" ") : "-";
		var locality = countryCode;
		var industry = (typeof profile.industry !== 'undefined')? profile.industry.split(",").join(" ") : "-";
		var company = (typeof profile.experience[0].organization[0].name !== 'undefined')? profile.experience[0].organization[0].name.split(",").join(" ") : "-";
		var job_title = (typeof profile.experience[0].title !== 'undefined')? profile.experience[0].title.split(",").join(" ") : "-";
		var job_from = (typeof profile.experience[0].start !== 'undefined')? profile.experience[0].start.split(",").join(" ") : "-";
		var current_to = (typeof profile.experience[0].end !== 'undefined')? profile.experience[0].end.split(",").join(" ") : "-";
		var company_id = (typeof profile.experience[0].organization[0].unique_id !== 'undefined') ? profile.experience[0].organization[0].unique_id : "-";
		var company_url = (typeof profile.experience[0].organization[0].profile_url !== 'undefined') ? profile.experience[0].organization[0].profile_url.split(",").join(" ") : "-";
		var linkedin_id = (typeof profile.linkedin_id !== 'undefined') ? profile.linkedin_id : "-";
		var picture_url = (typeof profile.image_url !== 'undefined') ? profile.image_url : "-";
		var languages = (typeof profile.languages !== 'undefined') ? profile.languages.length : "0";
		var recommendations = (typeof profile.recommendations !== 'undefined')? profile.recommendations.length : "0";
		var last_visited = (typeof profile.last_visited !== 'undefined') ? profile.last_visited : "-";
		var updated = (typeof profile.updated !== 'undefined') ? profile.updated : "-";
		var headline = (typeof profile.headline !== 'undefined') ? profile.headline.split(",").join(" ") : "-";
		var duration = (typeof profile.experience[0].duration !== 'undefined') ? profile.experience[0].duration : "-";
		var job_location = (typeof profile.experience[0].location !== 'undefined') ? profile.experience[0].location.split(",").join(" ") : "-";
		var job_description = (typeof profile.experience[0].description !== 'undefined') ? profile.experience[0].description.split(",").join(" ") : "-";
		
		var insert = "INSERT INTO SG_LEADS_UTF8.singapore (idleads,first_name,last_name,fullname,profile_url,connections,location,industry,company,job_title,job_from,current_to,company_id,company_url,linkedin_id,picture_url,languages,recommendations,last_visited,updated,headline,duration,job_location,job_description) VALUES (DEFAULT," + connection.escape(first_name) + "," +  connection.escape(last_name) +"," + connection.escape(fullname) +","+connection.escape(profile_url)+","+connection.escape(connections)+","+connection.escape(locality) + "," + connection.escape(industry) + "," + connection.escape(company) + "," + connection.escape(job_title) + "," + connection.escape(job_from) + "," + connection.escape(current_to) + "," + connection.escape(company_id) + "," + connection.escape(company_url) + "," + connection.escape(linkedin_id) + "," + connection.escape(picture_url) + "," + connection.escape(languages) + "," + connection.escape(recommendations) + "," + connection.escape(last_visited) + "," + connection.escape(updated) + "," + connection.escape(headline) + "," + connection.escape(duration) + "," + connection.escape(job_location) + "," + connection.escape(job_description) +");"
			connection.query(insert,function(err,rows,fields){
				if(err){ console.log(err); callback(true);}
				else callback(true);
			});
	}
	else{
		callback(true);
	}	


}
