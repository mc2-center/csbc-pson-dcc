"""Truncate Synapse team.

This script will remove all team members from a given Synapse team
ID (-t), with the exception of:

  * Julie Bletz (jbletz, 3361792)
  * Ashley Clayton (ashley.clayton, 3408068)
  * Amber Nelson (ambernelson, 3419821)
  * Verena Chung (vchung, 3393723)

author: verena.chung
"""

import os
import argparse
import getpass

import synapseclient


def login():
    """Log into Synapse. If env variables not found, prompt user.

    Returns:
        syn: Synapse object
    """
    try:
        syn = synapseclient.login(
            os.getenv('SYN_USERNAME'),
            apiKey=os.getenv('SYN_APIKEY'),
            silent=True)
    except synapseclient.core.exceptions.SynapseNoCredentialsError:
        print("Credentials not found; please manually provide your",
              "Synapse username and password.")
        username = input("Synapse username: ")
        password = getpass.getpass("Synapse password: ")
        syn = synapseclient.login(username, password, silent=True)
    return syn


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(
        description=("Removes all non-manager team members from a given "
                     "Synapse team ID."))
    parser.add_argument("-t", "--team_id",
                        type=int, required=True,
                        help="Synapse team ID, e.g. 3424242")
    return parser.parse_args()


def truncate_members(syn, team_id):
    """Remove all non-manager Synapse users from given team."""

    # Synapse user IDs for Julie, Amber, Ashley, and Verena - DO NOT REMOVE FROM TEAM!
    manager_ids = ["3361792", "3408068", "3419821", "3393723"]

    count = 0
    team_members = [m.get('member') for m in syn.getTeamMembers(team_id)]
    for user in team_members:
        user_id = user.get('ownerId')
        if user_id not in manager_ids:
            syn.restDELETE(f"/team/{team_id}/member/{user_id}")
            count += 1

    # Output mini-summary report.
    team = syn.getTeam(team_id)
    print(f"Removed {count} members from team: {team.get('name')}")


def main():
    """Main function."""
    syn = login()
    args = get_args()

    truncate_members(syn, args.team_id)


if __name__ == "__main__":
    main()
