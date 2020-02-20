#!/usr/bin/env python3
"""CSBC/PS-ON Project Descriptions

This will scrap for the project (and core) descriptions from each
CSBC/PS-ON participating institution on Synapse.

author: verena.chung
"""

import os
import re

import synapseclient


def parse_u01_project(page):
    """Parse U01 project description."""

    page = page.replace("**", "")
    try:
        description = re.search(
            r"Project Description.*?\s(.*?)->", page, re.S).group(1)
    except AttributeError:
        description = ""

    return description.rstrip("\n\n&nbsp;").strip()


def parse_information(center, page):
    """"""

    all_info = ""
    sections = re.split(r"\n####\*", page)
    for section in sections:
        section = section.replace("**", "").lstrip("&nbsp;")
        info = re.search(r"(\w+)\s*(\d)?:?\s*(.*?)\n+(.*)", section, re.S)
        title = info.group(3)
        all_info += "\t".join([center, info.group(1), info.group(2) or "",
                               title if title != "&nbsp;" else "",
                               repr(info.group(4).strip("&nbsp;\n\n").strip())]) + "\n"
    return all_info


def parse_u54_center(center, page):
    """Parse project and core descriptions from U54 center."""

    # Center has only project description. Add '###COR' to the end so that
    # all project descriptions can be captured.
    if not re.search(r"P(ROJECT|roject).*#CORE", page, re.S):
        page += "\n###COR"

    project_subpage = re.search(r"(Project.*)###COR", page, re.S).group(1)
    project_info = parse_information(center, project_subpage)

    core_info = None
    cores_found = re.search(r"CORE(S)?\s*(.*)", page, re.S)
    if cores_found:
        core_subpage = cores_found.group(2)
        core_info = parse_information(center, core_subpage)

    return project_info, core_info


def main():
    """Main function."""

    syn = synapseclient.login(silent=True)

    with open("projects_and_cores2.tsv", "w") as out:
        out.write("\t".join(["Center Name", "Project/Core", "Number",
                             "Title", "Description Markdown"]) + "\n")
        project_view = syn.tableQuery(
            "select id from syn10142562 where grantType in ('U01', 'U54')").asDataFrame()
        for syn_id in project_view.id:
            project = syn.get(syn_id, downloadFile=False)
            project_wiki = syn.getWikiHeaders(project)

            print(syn_id)  # diagnostic.

            # U01 grants only has one project and the description is
            # listed on the main Wiki page.
            if project.grantType[0] == "U01":
                project_page_id = project_wiki[0].id
                project_page = syn.getWiki(project, project_page_id).markdown
                project_desc = parse_u01_project(project_page)
                out.write("\t".join([project.centerName[0], "Project", "1",
                                     "", repr(project_desc)]) + "\n")

            # Whereas U54 grants can have multiple projects (and cores),
            # which are listed on another page.
            else:
                for page in project_wiki:
                    if re.search(r"Projects.*Core", page.title):
                        project_page_id = page.id
                        break
                project_page = syn.getWiki(project, project_page_id).markdown
                projects, cores = parse_u54_center(
                    project.centerName[0], project_page)
                out.write(projects)
                if cores:
                    out.write(cores)

    syn.logout()


if __name__ == "__main__":
    main()
