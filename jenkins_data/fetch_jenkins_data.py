import sys
import jenkins

if __name__ == "__main__":

    server = jenkins.Jenkins('https://builds.apache.org/')

    try:
        # Sometimes connecting to Jenkins server is banned due to ill use of API
        # Test connection to server
        print(f"Jenkins-API version: {server.get_version()}")


    except OSError:
        print("Error connecting to server")
        sys.exit(1)

    if len(sys.argv != 1):
        print("Provide path to one csv file with projects' names in it.") 
        sys.exit(1)

    try:
        