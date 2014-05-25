
if __name__ == "__main__":
    if sys.argv[1] == 'step1':
        run_step1(sys.argv[2])
    elif sys.argv[1] == 'step2':
        run_step2(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == 'stepN':
        run_step3()
    else:
        raise Exception()
