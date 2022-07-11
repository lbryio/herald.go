import argparse
import io
import json
import os
import sys
import unittest
from datetime import date, datetime
from getpass import getpass

try:
    import github3
except ImportError:
    print('To run release tool you need to install github3.py:')
    print('')
    print('  $ pip install github3.py')
    print('')
    sys.exit(1)

AREA_RENAME = {
    'api': 'API',
    'dht': 'DHT'
}


def build_upload_binary(release: github3.repos.release.Release) -> None:
    # os.chdir(absolute_path)
    # os.system("go build .")
    cmd = f'CGO_ENABLED=0 go build -v -ldflags "-X github.com/lbryio/herald/meta.Version={release.name}"'
    print(cmd)
    # os.system("go build .")
    os.system(cmd)
    with open("./hub", "rb") as f:
        release.upload_asset("binary", "hub", f)


def get_github():
    config_path = os.path.expanduser('~/.lbry-release-tool.json')
    if os.path.exists(config_path):
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
            return github3.github.GitHub(token=config['token'])

    token = os.environ.get("GH_TOKEN")
    if not token:
        print('GitHub Credentials')
        username = input('username: ')
        password = getpass('password: ')
        gh = github3.github.GitHub(username, password)
        token = input('Enter 2FA: ')
        with open(config_path, 'w') as config_file:
            json.dump({'token': token}, config_file)
        gh.login(token=token)

    return gh


def get_labels(pr, prefix):
    for label in pr.labels:
        label_name = label['name']
        if label_name.startswith(f'{prefix}: '):
            yield label_name[len(f'{prefix}: '):]


def get_label(pr, prefix):
    for label in get_labels(pr, prefix):
        return label


BACKWARDS_INCOMPATIBLE = 'backwards-incompatible:'
RELEASE_TEXT = 'release-text:'
RELEASE_TEXT_LINES = 'release-text-lines:'


def get_backwards_incompatible(desc: str):
    for line in desc.splitlines():
        if line.startswith(BACKWARDS_INCOMPATIBLE):
            yield line[len(BACKWARDS_INCOMPATIBLE):]


def get_release_text(desc: str):
    in_release_lines = False
    for line in desc.splitlines():
        if in_release_lines:
            yield line.rstrip()
        elif line.startswith(RELEASE_TEXT_LINES):
            in_release_lines = True
        elif line.startswith(RELEASE_TEXT):
            yield line[len(RELEASE_TEXT):].strip()
            yield ''


class Version:

    def __init__(self, major=0, date=datetime.now(), micro=0, alphabeta=""):
        self.major = int(major)
        self.date = date
        self.micro = int(micro)
        self.alphabeta = alphabeta

    @classmethod
    def from_string(cls, version_string):
        (major, datemicroalphabeta) = version_string.split('.', 1)
        parts = datemicroalphabeta.split("-")
        if len(parts) > 1:
            datemicro, alphabeta = parts[0], parts[1]
        else:
            datemicro, alphabeta = parts[0], ""

        date, micro = datemicro.rsplit('.', 1) if datemicro.count('.') > 2 else (datemicro, "0")
        return cls(major.replace("v", ""), datetime.strptime(date, "%Y.%m.%d"), int(micro), alphabeta)

    @classmethod
    def from_content(cls, content):
        src = content.decoded.decode('utf-8')
        return cls.from_string(src)

    def increment(self, action):
        cls = self.__class__

        if action == 'major':
            return cls(self.major + 1, datetime.now(), self.micro, self.alphabeta)
        elif action == 'date':
            return cls(self.major, datetime.now(), self.micro, self.alphabeta)
        elif action == 'micro':
            return cls(self.major, datetime.now(), self.micro + 1, self.alphabeta)

        raise ValueError(f'unknown action: {action}')

    @property
    def tag(self):
        return f'{self}'

    def __str__(self):
        arr = ['v', self.major, '.', self.date.strftime("%Y.%m.%d")]
        if self.micro > 0:
            arr += [".", self.micro]
        if self.alphabeta != "":
            arr += ["-", self.alphabeta]
        return ''.join(str(p) for p in arr)


def get_draft_prerelease_vars(args) -> (bool, bool):
    draft = True
    prerelease = False
    if args.confirm > 2:
        draft = False
    elif args.confirm == 2:
        draft = False
        prerelease = True
    return draft, prerelease


def release(args):
    gh = get_github()
    repo = gh.repository('lbryio', 'hub')
    try:
        version_file = repo.file_contents('version.txt')
        current_version = Version.from_content(version_file)
        print(f'Current Version: {current_version}')
    except:
        current_version = Version()
        version_file = repo.create_file("version.txt", message="add version file",
                                        content=str(current_version).encode('utf-8'))

    if args.confirm <= 0:
        print("\nDRY RUN ONLY. RUN WITH --confirm TO DO A REAL RELEASE.\n")

    if args.action == 'current':
        new_version = current_version
    else:
        new_version = current_version.increment(args.action)
    print(f'    New Version: {new_version}')

    tag = args.start_tag if args.start_tag else current_version.tag

    try:
        previous_release = repo.release_from_tag(tag)
    except github3.exceptions.NotFoundError:
        previous_release = list(repo.releases())[-1]
        print(f' Changelog From: {previous_release.tag_name} ({previous_release.created_at})')
        print()

    incompats = []
    release_texts = []
    unlabeled = []
    fixups = []
    areas = {}
    for pr in gh.search_issues(f"merged:>={previous_release._json_data['created_at']} repo:lbryio/herald"):
        area_labels = list(get_labels(pr, 'area'))
        type_label = get_label(pr, 'type')
        if area_labels and type_label:
            for area_name in area_labels:
                for incompat in get_backwards_incompatible(pr.body or ""):
                    incompats.append(f'  * [{area_name}] {incompat.strip()} ({pr.html_url})')
                for release_text in get_release_text(pr.body or ""):
                    release_texts.append(release_text)
                if type_label == 'fixup':
                    fixups.append(f'  * {pr.title} ({pr.html_url}) by {pr.user["login"]}')
                else:
                    area = areas.setdefault(area_name, [])
                    area.append(f'  * [{type_label}] {pr.title} ({pr.html_url}) by {pr.user["login"]}')
        else:
            unlabeled.append(f'  * {pr.title} ({pr.html_url}) by {pr.user["login"]}')

    area_names = list(areas.keys())
    area_names.sort()

    body = io.StringIO()
    w = lambda s: body.write(s + '\n')

    w(f'## [{new_version}] - {date.today().isoformat()}')
    if release_texts:
        w('')
        for release_text in release_texts:
            w(release_text)
    if incompats:
        w('')
        w(f'### Backwards Incompatible Changes')
        for incompat in incompats:
            w(incompat)
    for area in area_names:
        prs = areas[area]
        area = AREA_RENAME.get(area.lower(), area.capitalize())
        w('')
        w(f'### {area}')
        for pr in prs:
            w(pr)

    print(body.getvalue())

    if unlabeled:
        w('')
        print('The following PRs were unlabeled and *will* be included in changelog:')
        for notskipped in unlabeled:
            print(notskipped)
            w(notskipped)

    if fixups:
        print('The following PRs were marked as fixups and not included in changelog:')
        for skipped in fixups:
            print(skipped)

    draft, prerelease = get_draft_prerelease_vars(args)
    if args.confirm > 0:

        commit = version_file.update(
            new_version.tag,
            version_file.decoded.decode('utf-8').replace(str(current_version), str(new_version)).encode()
        )['commit']

        release = None
        if args.action != "current":
            repo.create_tag(
                tag=new_version.tag,
                message=new_version.tag,
                sha=commit.sha,
                obj_type='commit',
                tagger=commit.committer
            )

            release = repo.create_release(
                new_version.tag,
                name=new_version.tag,
                body=body.getvalue(),
                draft=draft,
                prerelease=prerelease
            )

            build_upload_binary(release)
        elif args.action == "current":
            try:
                print(new_version.tag)
                # if we have the tag and release already don't do anything
                release = repo.release_from_tag(new_version.tag)
                if release.prerelease:
                    build_upload_binary(release)
                    release.edit(prerelease=False)
                else:
                    build_upload_binary(release)
                return
            except Exception as e:
                print(e)
                try:
                    # We need to do this to get draft and prerelease releases
                    release = repo.releases().next()
                    # Case me have a release and no tag
                    if release.name == new_version.tag:
                        release.edit(prerelease=prerelease, draft=draft)
                        build_upload_binary(release)
                        return
                    else:
                        raise Exception("asdf")
                except:
                    repo.create_tag(
                        tag=new_version.tag,
                        message=new_version.tag,
                        sha=commit.sha,
                        obj_type='commit',
                        tagger=commit.committer
                    )

                release = repo.create_release(
                    new_version.tag,
                    name=new_version.tag,
                    body=body.getvalue(),
                    draft=draft,
                    prerelease=prerelease
                )
            finally:
                if release:
                    build_upload_binary(release)


class TestReleaseTool(unittest.TestCase):

    def test_version_parsing(self):
        self.assertTrue(str(Version.from_string('v1.2020.01.01-beta')), 'v1.2020.01.01-beta')
        self.assertTrue(str(Version.from_string('v1.2020.01.01.10')), 'v1.2020.01.01-beta')

    def test_version_increment(self):
        v = Version.from_string('v1.2020.01.01-beta')
        self.assertTrue(str(v.increment('major')), 'v2.2020.01.01.beta')
        self.assertTrue(str(v.increment('date')), f'v1.{datetime.now().strftime("YYYY.MM.dd")}-beta')


def test():
    runner = unittest.TextTestRunner(verbosity=2)
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestReleaseTool)
    return 0 if runner.run(suite).wasSuccessful() else 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--confirm", default=0, action="count",
                        help="without this flag, it will only print what it will do but will not actually do it")
    parser.add_argument("--start-tag", help="custom starting tag for changelog generation")
    parser.add_argument("action", choices=['test', 'current', 'major', 'date', 'micro'])
    args = parser.parse_args()

    if args.action == "test":
        code = test()
    else:
        code = release(args)

    print()
    return code


if __name__ == "__main__":
    sys.exit(main())
