import os, sys
from PIL import Image, UnidentifiedImageError
from scaler import SchedulerClusterCombo, Client

def process(path: str):
    try:
        im = Image.open(path)
    except UnidentifiedImageError:
        return # ignore non-image files

    # resize the image if it's too big
    if im.width > 1024 or im.height > 1024:
        im.thumbnail((1024, 1024))

    # save as jpeg
    # this works because the workers are being run on the same machine as the client
    im.save(path, format="JPEG", quality=80)
    im.close()


def main():
    address = "tcp://127.0.0.1:2345"
    dir = sys.argv[1]

    _cluster = SchedulerClusterCombo(address=address, n_workers=6)
    client = Client(address=address)


    _results = client.map(process, [(os.path.join(dir, f),) for f in os.listdir(dir)])

if __name__ == "__main__":
    main()
