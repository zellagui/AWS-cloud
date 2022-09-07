const axios = require("axios");
const fs = require("fs");
const FormData = require('form-data');
var qs = require("qs");
const ffmpeg = "/opt/bin/ffmpeg";
const ffprobePath = "/opt/bin/ffprobe";
const { spawnSync } = require("child_process");


const AWS = require('aws-sdk');
const s3 = new AWS.S3({ apiVersion: '2006-03-01' });

let awsConfig = {
  'region': 'us-east-1',
  'endpoint': 'http://dynamodb.us-east-1.amazonaws.com',
};

AWS.config.update(awsConfig);

const documentClient = new AWS.DynamoDB.DocumentClient();

//user credential
const GROKVIDEO_USERNAME = "ENTER USERNAME";
const GROKVIDEO_PASSWORD = "ENTER PASSWORD";

const grokvideoApiBaseUrl = "dev-atg-mm.grokvideo.ai"; 
const tableName = 'oa-plugin-grokvideo-video';



exports.handler = async (event) => {
  
//  Part 1:   Event handeling - item retrieve - video verification
    console.log(JSON.stringify(event,null,4));
    //Trailer informatiom
    const title = "a title";
    const description = "uploaded from lambda/s3";
    const language = "FRENCH";

    //Get source ID  and user ID
    const key = event.Records[0].s3.object.key;
    const parts = key.split("/");
    const userId = parts[1];
    const sourceId = parts[2];
    const filename = parts[3];
    const filenameParts = filename.split(".");
    const videoId = filenameParts[0];
    
     //Bucket info and grokAPIurl
    const Bucket = "oa-plugin-grokvideo";
    const Key = `users/${userId}/${videoId}/${videoId}.mp4`;
    //const tableName = 'oa-plugin-grokvideo-video';

    //make sure that the video provided is a source video and not a trailers
    if (sourceId != videoId) {
        console.log("This is not a source");
        const response = {
          statusCode: 401,
          body: JSON.stringify('This is not a source'),
        };
    return response;    
    }
  
    
    
    
//  Part 2: Get item from Db - Connect to Grok API and post video - update movie ID
    const videoData = await getItemById(sourceId, tableName);
    console.log('videoData: ' + videoData);

    //connection to GROK
    const credentials = await get_credentials(grokvideoApiBaseUrl, GROKVIDEO_USERNAME, GROKVIDEO_PASSWORD);
    
    console.log(credentials);
    
    //GET movie URL
    const movie_url = await getFileUrl(Key, Bucket);
    
    console.log('movie URL:     ' + movie_url);
    
    //post source to GROK
    let postGrok = await post_url(title, description, language, movie_url, credentials, grokvideoApiBaseUrl);
    
    console.log(postGrok);
    
    //extract grok_movie_id
    let grok_movie_id = postGrok.movie_id;

    console.log('grok video movie :   ' + grok_movie_id);
    
    //update value of grok_movie_id provided by grok
    await updateMovieId(sourceId, grok_movie_id);
    
    const updatedTable = await getItemById(sourceId, tableName);
    
    console.log('videoData: ' + updatedTable);
    

    
    
    
//  Part 3: Get video from S3 - generate tmp file name - save video to tmp directory
    console.log('bucket & key: ' + Bucket, Key);
    
    const downloadResult = await getVideoFromS3(Bucket, Key);
    
    console.log(downloadResult);
    
    //template for tpm file name
    const template_video = "/tmp/vid-{HASH}.mp4";
    const tmp_video_path = await generate_tmp_file_path(template_video);
    //console.log(tmp_video_path);
   
    
   
    let save_tmp_video = await save_tmp_file(downloadResult.Body, template_video);
    //console.log('result download body:  ' + downloadResult.Body);
    

  //  console.log('!!!!video duration:      ' + getVideoDuration(save_tmp_video));



//part 4: ffmpeg
   const template_thumbnail = "/tmp/thumbnail-{HASH}.jpg";
   const tmp_thumbnail_path = await generate_tmp_file_path(template_thumbnail);
   
   const targetSecond = 2;
   //console.log(tmp_thumbnail_path);

   console.log(await file_exist(tmp_thumbnail_path));
   

  const screenshot = await createImageFromVideo(tmp_video_path, tmp_thumbnail_path, targetSecond);
  
  
  
  console.log('thumbnails saved in tmp:   ' + screenshot);

  await uploadFileToS3(tmp_video_path, Key, Bucket);


    const response = {
        statusCode: 200,
        body: JSON.stringify('video posted to GROK'),
    };
    return response;
};


async function get_credentials(grokvideoApiBaseUrl, GROKVIDEO_USERNAME, GROKVIDEO_PASSWORD) {
  console.log("getting credentials");
  const credential_response = await axios({
    method: "post",
    url: `https://${grokvideoApiBaseUrl}/auth/login`,
    data: qs.stringify({ email: GROKVIDEO_USERNAME, password: GROKVIDEO_PASSWORD }),
  });
  return credential_response.data.result;
}

async function getFileUrl(key, bucket){
  console.log("getting file URL");
  console.log('!KEY!  :' + key);
  const params = {
    Bucket: bucket,
    Key: key,
    Expires: 60 * 60 * 1,
  };
  return await s3.getSignedUrlPromise("getObject", params);
}

async function post_url(title, description, language, movie_url, credentials, grokvideoApiBaseUrl) {
  try {
    var data = new FormData();
    data.append('title', title);
    data.append('desccription', description);
    data.append('language', language);
    data.append('movie_url', movie_url);

    var config = {
      method: 'post',
      url: `https://${grokvideoApiBaseUrl}/v3/api/movies`,
      headers: {
        'Authorization': `${credentials.token_type} ${credentials.access_token}`,
        ...data.getHeaders()
      },
      data: data
    };

    const resp = await axios(config);
    //const resp = await axios.post('https://jsonplaceholder.typicode.com/posts', newPost);
    return resp.data;
  }
  catch (err) {
    // Handle Error Here
    console.error(err);
  }
}

//query video data by ID
async function getItemById(id, tableName) {
  let params = {
    TableName: tableName,
    Key: { id }
  };
  let promise = documentClient.get(params).promise();
  let result = await promise;
  //console.log(result);
  if (!result.Item) {
    const msg = `Cannot find item with id=${id} in table ${tableName}`;
    console.error(msg);
    throw { statusCode: 404, body: { status: 'error', msg } };
  }
  return JSON.stringify(result.Item);
}

//update grok-movie-id
async function updateMovieId(video_id, grok_movie_id) {
  const params = {
            TableName: 'oa-plugin-grokvideo-video',
            Key:{ "id":  video_id, },
            UpdateExpression: 'SET grok_movie_id = :u',
            ExpressionAttributeValues:{
            ':u': grok_movie_id
            }
          };
        await documentClient.update(params).promise();
        console.log('grok_movie_id attributed succesfully');    
}

const getVideoFromS3 = async (triggerBucketName, fileName) => {
	const res = await s3.getObject({
		Bucket: triggerBucketName,
		Key: fileName
	}).promise();

	return res;
};

//Generate tmp file name in this format: "/tmp/vid-{HASH}.mp4"
const generate_tmp_file_path = async (filePathTemplate) => {
  const charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
	let result = "";
	const len = 10;

	for (let i = len; i > 0; --i) {
		result += charset[Math.floor(Math.random() * charset.length)];
	}
	
	const tmpFilePath = filePathTemplate.replace("{HASH}", result);
	return tmpFilePath;
};

//Save file to tmp directory
const save_tmp_file = async (fileAsBuffer, template) => {
   const tmpVideoFilePath = await generate_tmp_file_path(template);
   await fs.promises.writeFile(tmpVideoFilePath, fileAsBuffer, "base64");
   console.log(`video downloaded to ${tmpVideoFilePath}`);
   return tmpVideoFilePath;
};

const file_exist = async (tmpFilePath) => {
  if (fs.existsSync(tmpFilePath)) {
		const stats = fs.statSync(tmpFilePath);
		const fileSizeInBytes = stats.size;

		if (fileSizeInBytes > 0) {
            console.log(`${tmpFilePath} exists!`); 
			return true;
		} else {
			console.error(`${tmpFilePath} exists but is 0 bytes in size`);
			return false;
		}
	} else {
		console.error(`${tmpFilePath} does not exist`);
		return false;
	}
};


//Generate thumbail image
const createImageFromVideo = async (tmp_video_path, tmp_thumbnail_path, targetSecond) => {
  try{
    const ffmpegParams =  createFfmpegParams(tmp_video_path, tmp_thumbnail_path, targetSecond);
    console.log(ffmpegParams);
    spawnSync(ffmpeg, ffmpegParams);
    return tmp_thumbnail_path;
  }catch(err){
    console.log('error while image creation');
  }
};


//generate thumbnail paramters
const createFfmpegParams =  (tmp_video_path, tmp_thumbnail_path, targetSecond) => {
  console.log('tmp thub:' + tmp_thumbnail_path);
  return [
        "-ss", targetSecond,
        "-i", tmp_video_path,
        "-vf", "thumbnail,scale=113:200",
        "-vframes", 1,
        tmp_thumbnail_path
    ];
};

//upload thumbnail image to S3
const uploadFileToS3 = async (tmpThumbnailPath, Key, Bucket) => {
  try{
    const jpgExtension = Key.replace(".mp4", "jpg");
    console.log(jpgExtension);
    
    const contents = fs.createReadStream(tmpThumbnailPath);
    console.log('content:   ' + JSON.stringify(contents));
    //console.log('tmp thub path:   ' + tmpThumbnailPath);
    const uploadParams = {
        Bucket: Bucket,
        Key: jpgExtension,
        Body: contents,
        ContentType: "image/jpg"
    };
    await s3.putObject(uploadParams).promise();
    console.log('thumbnail saved is S3');
      
    }catch(err){
      console.error('error in the uploadFileToS3(): ' + err);
    }
};

//Get video duration with ffprobe
const getVideoDuration = (tmp_video_path) => {
  const ffprobe = spawnSync(ffprobePath, [
     "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=nw=1:nk=1",
        tmp_video_path
    ]);
    return Math.floor(ffprobe.stdout.toString());
};
