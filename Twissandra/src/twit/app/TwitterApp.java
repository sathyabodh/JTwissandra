package twit.app;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import twit.schema.SchemaManager;

public class TwitterApp {
	SchemaManager manager = new SchemaManager();
	
	public static void main(String[] args) {
		
		try {
			new TwitterApp().run();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void run() throws IOException{
		
		manager.initialise();
		
		InputStreamReader converter = new InputStreamReader(System.in);
		BufferedReader in = new BufferedReader(converter);
		boolean isRunning = true ;
		while(isRunning)
		{
			System.out.println("Menu");
			System.out.println("1. Add User");
			System.out.println("2. Add Followersr");
			System.out.println("3. Add Following");
			System.out.println("4. Add Tweet");
			System.out.println("5. Show Tweet from Following");
			System.out.println("6. Quit");
			
			System.out.println("Enter Choice: ");
			String choice;
			int c = -1 ;
			choice = in.readLine();
			c = Integer.parseInt(choice);

			switch(c)
			{
				case 1: 
					System.out.println("Enter UserName and password ");
					String userName = in.readLine();
					String password = in.readLine();
					manager.createUser(userName, password);
					break ;
				
				case 2: 
					System.out.println("Add username and follower name");
					userName = in.readLine();
					String follower = in.readLine();
					manager.addFollower(userName, follower);
					break;
				
				case 3: 
					System.out.println("Add username and following name");
					userName = in.readLine();
					String following = in.readLine();
					manager.addFollower(userName, following);
					break;

				case 4:
					System.out.println("Enter Username and tweet");	
					userName = in.readLine() ;
					String tweet = in.readLine();
					manager.addTweet(userName, tweet);
					break;
	
				case 5:
					System.out.println("Enter Username for showing tweet from following");	
					userName = in.readLine() ;
					break;
				
				case 6: isRunning = false ;
					break;
				
				default: System.out.println("Invalid choice");
			}
			
		}
		
	}
	
}
