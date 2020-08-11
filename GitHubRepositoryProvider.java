package com.alation.repo.github;

import static com.alation.git.github.util.GitHubConstants.*;

import java.io.BufferedWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alation.api.utils.ApiUtils;
import com.alation.api.utils.ConfigHelper;
import com.alation.api.utils.PropHelper;
import com.alation.git.github.util.*;
import com.alation.github.http.AlationProcessor;
import com.alation.github.metadata.MetadataHandle;
import com.alation.github.metadata.MetadataHandler;
import com.alation.github.model.AlationBulkMetadataUpdate;
import com.alation.github.model.AlationConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javaparser.JavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.visitor.VoidVisitorAdapter;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.http.HttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class GitHubRepositoryProvider {
	// Static Stuff
	private static final Logger logger = Logger.getLogger(GitHubRepositoryProvider.class);

	List<AlationBulkMetadataUpdate> alationBulkMetaDataHandle = new ArrayList<AlationBulkMetadataUpdate>();
	private MetadataHandler handler = new MetadataHandler();
	// Configuration Helper
	private static ConfigHelper configHelper = new ConfigHelper();

	private static JSONArray FileContentArr = new JSONArray();

	public static void main(String[] args) throws Exception {

		// get all repos this
	
		GitHubRepositoryProvider connectorpro = new GitHubRepositoryProvider();

		
		getConfigHelper().createConfig();
		

		connectorpro.getRepository();
		
	

	}

	public void getRepository() throws Exception {
		JSONArray repos = null;
		LinkedList<String> ReponmList = new LinkedList<String>();
		String accessToken = GitHubConfig.getAccessToken();
		JSONArray repoContent = new JSONArray();
		JSONArray contentList = new JSONArray();

		HttpResponse response = null;

		String repositoryOwner = GitHubConfig.getrepositoryOwner();
		String Reposiory_url = GITHUB_USER_URL + "/" + repositoryOwner + "/" + REPOSITORY_PARTIAL_URL;
		repos = GitHubConfig.get_all_repos(accessToken, Reposiory_url);

		ReponmList = getReponameList(repos);

		JSONArray getRepoContenttree = GitHubConfig.getRepoFileContentTree(ReponmList);
		repoContent = folderContent(ReponmList, repos);
		contentList = subFolderContent(repoContent, ReponmList, getRepoContenttree);
		System.out.println("FileContentArr in get Repository:::" + FileContentArr);

		// FileContent(getRepoContenttree, repoContent);

		response = postContentAlation(repoContent, contentList);

		//GithubUtils.BulkMetadataPosnewt(FileContentArr);

		if (response.getStatusLine().getStatusCode() == 200) {
			logger.info("Successfully posted in alation");

		}

	}

	private String findRepositoryName(String url) {
		// TODO Auto-generated method stub
		String foldername = new String();
		Pattern regex = Pattern.compile("repos/(.*?)/git");
		Matcher regexMatcher = regex.matcher(url);
		while (regexMatcher.find()) {
			for (int i = 1; i <= regexMatcher.groupCount(); i++) {
				String subdata = regexMatcher.group(1);

				foldername = subdata.split("/")[1];

			}
		}
		return foldername;
	}

	private HttpResponse postContentAlation(JSONArray repoContent, JSONArray contentList) throws Exception {
		StringBuffer jsonForFolder = new StringBuffer();
		StringBuffer jsonForContent = new StringBuffer();
		StringBuffer folderAndContent = new StringBuffer();
		HttpResponse response = null;

		// TODO Auto-generated method stub
		for (int i = 0; i < repoContent.size(); i++) {
			JSONObject repo = (JSONObject) repoContent.get(i);

			jsonForFolder.append(repo);
			jsonForFolder.append(System.getProperty("line.separator"));
		}
		for (int j = 0; j < contentList.size(); j++) {

			JSONObject content = (JSONObject) contentList.get(j);

			jsonForContent.append(content);
			jsonForContent.append(System.getProperty("line.separator"));
		}

		folderAndContent = jsonForFolder.append(jsonForContent);

		response = ApiUtils.doPOST(new StringEntity(folderAndContent.toString(), ContentType.APPLICATION_JSON));
		return response;

	}

	public static LinkedList<String> getReponameList(JSONArray repos) {
		LinkedList<String> reponm = new LinkedList<String>();
		for (int i = 0; i < repos.size(); i++) {
			JSONObject repo = (JSONObject) repos.get(i);
			String reponame = repo.get(GitHubConstants.NAME).toString();
			reponm.add(reponame);
		}
		logger.info("RepoName:============" + reponm);
		return reponm;
	}

	public JSONArray folderContent(LinkedList<String> RepoList, JSONArray repos) {

		LinkedHashMap<String, String> requiredMap = new LinkedHashMap<String, String>();
		JSONArray repocontent = new JSONArray();

		for (int i = 0; i < repos.size(); i++) {

			JSONObject repo = (JSONObject) repos.get(i);
			JSONObject individual_repo = (JSONObject) repo.get(GitHubConstants.OWNER);
			String is_directoryflag;
			String reponame = repo.get(GitHubConstants.NAME).toString();
			requiredMap.put(GitHubConstants.NAME, reponame);
			if (!RepoList.isEmpty() && RepoList != null) {
				is_directoryflag = "true";
				requiredMap.put(GitHubConstants.IS_DIRECTORY, is_directoryflag);
			}
			String Owner = individual_repo.get(GitHubConstants.LOGIN).toString();
			requiredMap.put(GitHubConstants.OWNER, Owner);
			String group = individual_repo.get(GitHubConstants.TYPE).toString();
			requiredMap.put(GitHubConstants.GROUP, group);

			String ts_last_accessed = repo.get(GitHubConstants.CREATED_AT).toString();
			requiredMap.put(GitHubConstants.TS_LAST_ACCESSED, ts_last_accessed);
			String ts_last_modified = repo.get(GitHubConstants.UPDATED_AT).toString();
			requiredMap.put(GitHubConstants.TS_LAST_MODIFIED, ts_last_modified);
			requiredMap.put("path", "/");
			LinkedList<String> fieldnames = new LinkedList<String>();
			JSONObject requiredJson = new JSONObject();
			requiredJson.putAll(requiredMap);

			repocontent.add(i, requiredJson);

		}

		return repocontent;

	}

	public JSONArray subFolderContent(JSONArray repocontent, LinkedList<String> ReponmList, JSONArray getRepoContent) {
		JSONArray Arraycontent = new JSONArray();
		try {
			LinkedHashMap<String, String> requiredfields = new LinkedHashMap<String, String>();
			String foldername = "";
			String reponame = "";
			String reponame_content = "";
			String reponame_type = "";
			String is_directoryflag;
			JSONObject JSONcontent = null;
			String Content = new String();
			StringBuffer FilePath = null;
			String Filename = null;
			AlationProcessor alationProcessor = null;
			// JSONArray FileContentArr=new JSONArray() ;

			for (int j = 0; j < getRepoContent.size(); j++) {
				String FileSystemid = PropHelper.getHelper().getFilesystemid();

				JSONObject repoContent = (JSONObject) getRepoContent.get(j);
				String sha = repoContent.get("sha").toString();
				logger.info(repoContent);
				String url = repoContent.get("url").toString();
				logger.info(url);
				Pattern regex = Pattern.compile("repos/(.*?)/git");
				Matcher regexMatcher = regex.matcher(url);
				while (regexMatcher.find()) {
					for (int i = 1; i <= regexMatcher.groupCount(); i++) {
						String subdata = regexMatcher.group(1);

						foldername = subdata.split("/")[1];

					}
				}
				reponame_content = repoContent.get(GitHubConstants.PATH).toString();

				reponame_type = repoContent.get(GitHubConstants.TYPE).toString();
				requiredfields.put(GitHubConstants.NAME, reponame_content);
				requiredfields.put(GitHubConstants.TYPE, reponame_type);

				if (reponame_content.contains("/")) {
					// code convert to mtd
					Map<StringBuffer, String> PathNameMap = new HashMap<StringBuffer, String>();
					PathNameMap = GitHubConfig.getPathNameMap(reponame_content);
					// code
					for (Map.Entry<StringBuffer, String> entry : PathNameMap.entrySet()) {
						FilePath = entry.getKey();
						Filename = entry.getValue();
						requiredfields.put("path", "/" + foldername + "/" + FilePath);
						requiredfields.put(GitHubConstants.NAME, Filename);

					}

				} else {
					requiredfields.put("path", "/" + foldername + "/");
				}
				if (reponame_type.equalsIgnoreCase("blob")) {
					JSONArray FileContent = GitHubConfig.getRepoFileContent(reponame_type, reponame_content, sha,
							foldername);

					Content = GitHubConfig.decodeString(FileContent);

					String paths = writeContentToTempFile(reponame_content, Content, foldername);
					System.out.println("paths:::" + paths);
					// code
					String FullPath = GitHubConstants.PATH_CHAR + foldername + GitHubConstants.PATH_CHAR
							+ reponame_content;
					System.out.println("FullPath:::" + FullPath);
					if (paths.contains("java")) {

						JSONObject FileContentobj = convertToClassFile(paths, FullPath);
						System.out.println("FileContentobj::" + FileContentobj);
						FileContentArr.add(FileContentobj);

						System.out.println("FileContentArr:::" + FileContentArr);
					}

					// code
					
					
					  //code
					if(reponame_content.contains("csv"))  
					{ MetadataHandle handle =handler.getHandle(getAlationConfig(), paths); if (handle != null) {
					  alationBulkMetaDataHandle = processBulkMetdataUpdate(foldername,
					  reponame_content, handle, alationBulkMetaDataHandle); }
					  logger.info("Paths:::" + paths); String jsonString = ""; for
					  (AlationBulkMetadataUpdate alationBulkMetaDataUpdate :
					  alationBulkMetaDataHandle) { String jsonappendString = new ObjectMapper()
					  .writeValueAsString(alationBulkMetaDataUpdate); jsonappendString =
					  jsonappendString.replace("\n", "").replace("\r", ""); jsonString =
					  jsonString.concat(jsonappendString + System.lineSeparator()); } 
					  // Do bulk posting
   
                     alationProcessor.doBulkMetadataPost(jsonString);
					  
					  } //code
					 
				}
				for (int k = 0; k < repocontent.size(); k++) {
					JSONObject speratejson = (JSONObject) repocontent.get(k);

					reponame = speratejson.get(GitHubConstants.NAME).toString();
					if (reponame.equals(foldername)) {
						// logger.info(reponame);
						String Owner = speratejson.get(GitHubConstants.OWNER).toString();
						requiredfields.put(GitHubConstants.OWNER, Owner);
						String group = speratejson.get(GitHubConstants.GROUP).toString();
						requiredfields.put(GitHubConstants.GROUP, group);
						String ts_last_accessed1 = speratejson.get(GitHubConstants.TS_LAST_ACCESSED).toString();
						requiredfields.put(GitHubConstants.TS_LAST_ACCESSED, ts_last_accessed1);
						String ts_last_modified1 = speratejson.get(GitHubConstants.TS_LAST_MODIFIED).toString();
						requiredfields.put(GitHubConstants.TS_LAST_MODIFIED, ts_last_modified1);
						if (reponame_type.equalsIgnoreCase("tree")) {
							requiredfields.put(GitHubConstants.IS_DIRECTORY, "true");
						} else {
							requiredfields.put(GitHubConstants.IS_DIRECTORY, "false");
						}
						JSONcontent = new JSONObject();
						JSONcontent.putAll(requiredfields);

					}

				}
				Arraycontent.add(JSONcontent);

			}

		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		}
		return Arraycontent;
	}

	private List<AlationBulkMetadataUpdate> processBulkMetdataUpdate(String foldername, String reponame_content,
			MetadataHandle handle, List<AlationBulkMetadataUpdate> alationBulkMetaDataHandle2) throws Exception {

		AlationBulkMetadataUpdate metadataUpdate = new AlationBulkMetadataUpdate();
		metadataUpdate.setDescription(handle.getBody());

		// Cook up alation FS Object URI
		StringBuffer key4BulkUpdate = new StringBuffer(String.valueOf(PropHelper.getHelper().getFilesystemid()));
		key4BulkUpdate.append(GitHubConstants.PATH_CHAR);
		key4BulkUpdate.append(foldername);
		key4BulkUpdate.append(GitHubConstants.PATH_CHAR);
		key4BulkUpdate.append(reponame_content);
		metadataUpdate.setKey(key4BulkUpdate.toString());
		alationBulkMetaDataHandle.add(metadataUpdate);
		if (getAlationConfig().getFileSystemId() != null) {

		} else {
			logger.warn("Skipping file system post as filesystem_id is not provided in lambda");
		}

		return alationBulkMetaDataHandle;
		// TODO Auto-generated method stub

	}

	private String writeContentToTempFile(String name, String content, String foldername) throws Exception {
		String[] Filepath = null;

		String FullPath = GitHubConstants.PATH_CHAR + foldername + GitHubConstants.PATH_CHAR + name;

		if (name.contains("/")) {
			Filepath = name.split("/");
			name = Filepath[Filepath.length - 1];

		}

		File myTempFile = new File(Files.createTempDir(), name);
		String cname = myTempFile.getName();
		String tDir = System.getProperty("java.io.tmpdir");
		BufferedWriter bw = new BufferedWriter(new FileWriter(myTempFile));
		bw.write(content);
		bw.close();
		String path = myTempFile.getAbsolutePath();
		System.out.println("path in  writemtd:" + path);
		return path;
	}

	private JSONObject convertToClassFile(String path, String name) throws Exception {

		System.out.println("ConverttoClassFile  >>" + "file path" + path);
		JSONObject Filecontentobj = null;
		// if (path.contains("java")) {

		logger.info("name:::" + name);
		FileInputStream in = new FileInputStream(path);

		CompilationUnit cu;

		try {

			cu = JavaParser.parse(in);

		} finally {
			in.close();
		}
		MethodVisitor MV = new MethodVisitor();

		MV.visit(cu, null);
		System.out.println("OUTPUT::::" + MV.sb);

		// code
		Filecontentobj = fileContent(name, MV.sb);

		return Filecontentobj;

	}

	private JSONObject fileContent(String name, StringBuffer sb) throws Exception {
		// TODO Auto-generated method stub
		LinkedHashMap<String, String> requiredMap = new LinkedHashMap<String, String>();

		String FileSystemId = PropHelper.getHelper().getFilesystemid();
		String Key = FileSystemId + GitHubConstants.PATH_CHAR + name;

		requiredMap.put(GitHubConstants.KEY, Key);
		requiredMap.put(GitHubConstants.DESCRIPTION, sb.toString());

		Gson prettyGson = new GsonBuilder().setPrettyPrinting().create();
		String prettyJson = prettyGson.toJson(requiredMap);

		// Construct a JSONObject from a Map.
		JSONObject contentObject = new JSONObject(requiredMap);

		return contentObject;
		// code
	}



	public class MethodVisitor extends VoidVisitorAdapter {
		public String name = "";

		StringBuffer sb = new StringBuffer();

		@Override
		public void visit(MethodDeclaration n, Object arg) {
			name = n.getName().asString();
			sb.append(System.getProperty("line.separator"));
			sb.append(name);
			sb.append("<br/>  ");

		}

	}

	public AlationConfig getAlationConfig() {
		return getConfigHelper().getAlationConfig();
	}

	public static ConfigHelper getConfigHelper() {
		return configHelper;
	}

}
