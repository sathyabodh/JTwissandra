package twit.schema;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.UUID;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

public class SchemaManager 
{
	private Keyspace keySpace = null ;
	private ColumnFamilyTemplate<String, String> userMapping = null ;
	private ColumnFamilyTemplate<String, String> user = null ;
	private ColumnFamilyTemplate<String, String> following = null ;
	private ColumnFamilyTemplate<String, String> followers = null ;
	private ColumnFamilyTemplate<String, String> tweet = null ;
	private ColumnFamilyTemplate<String, String> userTweet = null ;
	private ColumnFamilyTemplate<String, String> timeLineTweet = null ;
	
	public void initialise()
	{
		
		createSchema() ;
		
		keySpace = HFactory.createKeyspace(SchemaConstants.KEYSPACE, getCluster());
		
		userMapping = new ThriftColumnFamilyTemplate<String, String>(keySpace, SchemaConstants.USER_MAP_TBL, 
				StringSerializer.get(), StringSerializer.get());

		user = new ThriftColumnFamilyTemplate<String, String>(keySpace, SchemaConstants.USER_TBL, 
				StringSerializer.get(), StringSerializer.get());
		
		followers = new ThriftColumnFamilyTemplate<String, String>(keySpace, SchemaConstants.FOLLOWERS_TBL, StringSerializer.get(), StringSerializer.get());
		
		following = new ThriftColumnFamilyTemplate<String, String>(keySpace, SchemaConstants.FOLLOWING_TBL, StringSerializer.get(), StringSerializer.get());
		
		tweet = new ThriftColumnFamilyTemplate<String, String>(keySpace, SchemaConstants.TWEET_TBL, StringSerializer.get(), StringSerializer.get());
		
		userTweet = new ThriftColumnFamilyTemplate<String, String>(keySpace, SchemaConstants.USER_TWEET_TBL, StringSerializer.get(), StringSerializer.get());
		
		timeLineTweet = new ThriftColumnFamilyTemplate<String, String>(keySpace, SchemaConstants.TIMELINE_TWEET, StringSerializer.get(), StringSerializer.get());
			
	}
	
	public void createSchema()
	{
		Cluster cluster = getCluster();
		
		KeyspaceDefinition keySpaceDef = cluster.describeKeyspace(SchemaConstants.KEYSPACE); 
		
		if(keySpaceDef != null)
			return ;
		
		ColumnFamilyDefinition usersMapCf= HFactory.createColumnFamilyDefinition(SchemaConstants.KEYSPACE, SchemaConstants.USER_MAP_TBL, ComparatorType.UTF8TYPE);
		
		ColumnFamilyDefinition usersCf= HFactory.createColumnFamilyDefinition(SchemaConstants.KEYSPACE, SchemaConstants.USER_TBL, ComparatorType.UTF8TYPE);
		
		ColumnFamilyDefinition followers = HFactory.createColumnFamilyDefinition(SchemaConstants.KEYSPACE, SchemaConstants.FOLLOWERS_TBL, ComparatorType.UTF8TYPE);
		
		ColumnFamilyDefinition following = HFactory.createColumnFamilyDefinition(SchemaConstants.KEYSPACE, SchemaConstants.FOLLOWING_TBL, ComparatorType.UTF8TYPE);
		
		ColumnFamilyDefinition tweet = HFactory.createColumnFamilyDefinition(SchemaConstants.KEYSPACE, SchemaConstants.TWEET_TBL, ComparatorType.UTF8TYPE);
		
		ColumnFamilyDefinition userTweet = HFactory.createColumnFamilyDefinition(SchemaConstants.KEYSPACE, SchemaConstants.USER_TWEET_TBL, ComparatorType.UTF8TYPE);
		
		ColumnFamilyDefinition timeLine = HFactory.createColumnFamilyDefinition(SchemaConstants.KEYSPACE, SchemaConstants.TIMELINE_TWEET, ComparatorType.UTF8TYPE);
		
		keySpaceDef = HFactory.createKeyspaceDefinition(SchemaConstants.KEYSPACE, ThriftKsDef.DEF_STRATEGY_CLASS, 1, Arrays.asList(usersMapCf,usersCf, following, followers, tweet, userTweet, timeLine));
		
		cluster.addKeyspace(keySpaceDef, true);
		
		
	}
	
	
	public Cluster getCluster()
	{
//		return HFactory.getOrCreateCluster("Test Cluster", "10.141.139.218:9160");
		return HFactory.getOrCreateCluster("Test Cluster", "10.141.139.133:9160");
	}
	

	public void createUser(String userName, String password)
	{
		ColumnFamilyUpdater<String, String> updater = userMapping.createUpdater(userName);
		String uuid = UUID.randomUUID().toString();
		updater.setString("uuid", uuid);
		
		userMapping.update(updater);
		
		ColumnFamilyUpdater<String, String> userUpdater = user.createUpdater(uuid);
		userUpdater.setString("uuid", uuid);
		userUpdater.setString("password", password);
		
		user.update(userUpdater);
	}
	
	public ColumnFamilyResult<String, String> getUserInfo(String userName)
	{
		ColumnFamilyResult<String, String> userMapR = userMapping.queryColumns(userName);
		
		ColumnFamilyResult<String, String> userR = user.queryColumns(userMapR.getString("uuid"));
		
		return userR ;
	}
	
	
	public void addFollower(String username, String friendName)
	{
		ColumnFamilyResult<String, String> friend = userMapping.queryColumns(friendName);
		
		ColumnFamilyResult<String, String> user = userMapping.queryColumns(username);
		
		ColumnFamilyUpdater<String, String> followersU = followers.createUpdater(user.getString("uuid"));
		followersU.setString(friend.getString("uuid"), "");
		followers.update(followersU);
		
	}
	
	public void addFollowing(String username, String friendName)
	{
		ColumnFamilyResult<String, String> friend = userMapping.queryColumns(friendName);
		
		ColumnFamilyResult<String, String> user = userMapping.queryColumns(username);
		
		ColumnFamilyUpdater<String, String> followingU = following.createUpdater(friend.getString("uuid"));
		followingU.setString(user.getString("uuid"), "");
		following.update(followingU);

	}
	
	public void addTweet(String userName, String tweetText)
	{
		ColumnFamilyResult<String, String> user = userMapping.queryColumns(userName);
		
		String tweetUUID = UUID.randomUUID().toString() ;
		
		ColumnFamilyUpdater<String, String> tweetU = tweet.createUpdater(tweetUUID);
		tweetU.setString("tweet_txt", tweetText);
		tweetU.setDate("tweetTime", new Date());
		tweetU.setString("user_id", user.getString("uuid"));
		tweet.update(tweetU);
		
		ColumnFamilyUpdater<String, String> userTweetU = userTweet.createUpdater(user.getString("uuid"));
		String timeInMilli = Long.toString(System.currentTimeMillis());
		userTweetU.setString(timeInMilli, tweetUUID);
		userTweet.update(userTweetU);
		
		ColumnFamilyResult<String, String> followersR = followers.queryColumns(user.getString("uuid"));
		Collection<String> columnNames = followersR.getColumnNames() ;
		
		System.out.println("Number of followers: " + columnNames.size());
		for(String userId: columnNames)
		{
			System.out.println("Adding tweet to " + userId);
			ColumnFamilyUpdater<String, String> timeLineTweetU = timeLineTweet.createUpdater(userId);
			userTweetU.setString(timeInMilli, tweetUUID);
			timeLineTweet.update(timeLineTweetU);
		}
	}

	public void showTweet(String userName)
	{
		ColumnFamilyResult<String, String> userMapR = userMapping.queryColumns(userName);
		
		System.out.println("My Tweets");
		ColumnFamilyResult<String, String> userTweetR = userTweet.queryColumns(userMapR.getString("uuid"));
		Collection<String> columns = userTweetR.getColumnNames();
		int i = 1 ;
		for(String cName : columns)
		{
			ColumnFamilyResult<String, String> tweetR = tweet.queryColumns(userTweetR.getString(cName));
			System.out.println("Tweet " + i + " : " + tweet);
		}
	}
}
