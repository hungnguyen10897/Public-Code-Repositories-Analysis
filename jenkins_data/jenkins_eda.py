# Simple script to understand structure of Jenkins, and its API

import jenkins

if __name__ == "__main__":

    server = jenkins.Jenkins('https://builds.apache.org/')

    