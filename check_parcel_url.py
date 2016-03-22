def check_parcel_url(url, parcelip, parcelport, debug):
    url = str(url)
    parcelip = str(parcelip)
    test = url.find(parcelip)
    if test == -1:
        url_vector = url.split('/')
        num_fields=len(url_vector)
        if debug==True:
            print 'num_fields: ' + str(num_fields)
        new_url_tail=''
        for i in range (4,num_fields+1):
            new_url_tail=new_url_tail + '/' + str( url_vector[i-1] )
            if debug==True:
                print 'new_url_tail: ' + new_url_tail
        new_url= str( url_vector[0] ) + '//' + str(parcelip) + ':' + str(parcelport) + new_url_tail
        print 'The parcelip is not contained in the url for the sample, attempting to fix'
        print '     changing this url: ' + url
        print '     to this url      : ' + new_url 
    else:
        new_url = url
    return new_url
