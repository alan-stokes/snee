import subprocess

def find_basestation():
    print('Finding basestation...')
    proc = subprocess.Popen(['motelist'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()

    start = out.find('/')
    end = out.find(' ', start, len(out))

    return out[start:end]

def main():
    print find_basestation()

if __name__ == "__main__":
	main()
