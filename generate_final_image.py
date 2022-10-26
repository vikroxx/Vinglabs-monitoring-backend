import cv2
import os
from PIL import ImageFont, ImageDraw, Image
from datetime import datetime


def generate_final_image(input_date, hist, pi, cumhist, bottle_processed, avg_bot_per_sec, max_bot_per_sec,
                         avg_non_food, avg_opaque, bottom_note=None, info=None,
                         base_file_path=os.path.join('images', 'schema.jpg')):
    if info == '':
        info = None
    # base_file_path = os.path.join('images', 'schema.jpg')
    # hist_path = os.path.join('images', '13102022_hist.jpeg')
    # pi_path = os.path.join('images', '13102022_pie.jpeg')
    # cum_hist_path = os.path.join('images', '13102022_cumhist.jpeg')

    # bottle_processed = 1621143
    # avg_bot_per_sec = 23.61
    # max_bot_per_sec = 11.52
    # avg_non_food = 4.5
    # avg_opaque = 1.2
    # bottom_note = 'The system was not operational between 19:00 - 21:00 local time'
    # input_date = '13102022'

    final_pdf_path = os.path.join('images', 'Vinglabs_Report_{}.pdf'.format(input_date))
    final_image_path = os.path.join('images', 'final_{}.jpg'.format(input_date))

    if 1000 < bottle_processed < 1000000:
        bottle_processed = str(round(bottle_processed / 1000, 1)) + ' K'
    elif bottle_processed <= 1000:
        bottle_processed = str(bottle_processed)
    elif bottle_processed > 1000000:
        bottle_processed = str(round(bottle_processed / 1000000, 3)) + ' M'
    avg_bot_per_sec = str(avg_bot_per_sec)
    max_bot_per_sec = str(max_bot_per_sec)
    avg_non_food = str(round(avg_non_food, 2)) + ' %'
    avg_opaque = str(round(avg_opaque, 2)) + ' %'

    # base_file, hist, pi, cumhist = cv2.imread(base_file_path), cv2.imread(hist_path), cv2.imread(pi_path), cv2.imread(
    #     cum_hist_path)

    base_file = cv2.imread(base_file_path)
    pi_resized = cv2.resize(pi, (int(1.37 * pi.shape[1]), int(1.37 * pi.shape[0])))
    width, height, _ = pi_resized.shape
    print(width, height)
    x, y = 1831, 1829
    base_file[int(x - width / 2): int(x + width / 2), int(y - height / 2):int(y + height / 2)] = pi_resized
    # cv2.imwrite(final_path_path, base_file)

    hist_resized = cv2.resize(hist, (int(1.36 * hist.shape[1]), int(1.36 * hist.shape[0])))
    width, height, _ = hist_resized.shape
    print(width, height)
    x, y = 1858, 707
    base_file[int(x - width / 2): int(x + width / 2), int(y - height / 2):int(y + height / 2)] = hist_resized
    cv2.imwrite(final_image_path, base_file)

    cumhist_resized = cv2.resize(cumhist, (int(1.38 * cumhist.shape[1]), int(1.38 * cumhist.shape[0])))
    width, height, _ = cumhist_resized.shape
    print(width, height)
    x, y = 2782, 1229
    base_file[int(x - width / 2): int(x + width / 2), int(y - height / 2):int(y + height / 2)] = cumhist_resized
    cv2.imwrite(final_image_path, base_file)

    image = Image.open(final_image_path)
    draw = ImageDraw.Draw(image)

    font = ImageFont.truetype("calibrib.ttf", 90)

    w, h = font.getsize(bottle_processed)
    draw.text((284 - int(w / 2), 1103 - int(h / 2)), bottle_processed, font=font)

    w, h = font.getsize(avg_bot_per_sec)
    draw.text((772 - int(w / 2), 1103 - int(h / 2)), avg_bot_per_sec, font=font)

    w, h = font.getsize(max_bot_per_sec)
    draw.text((1232 - int(w / 2), 1103 - int(h / 2)), max_bot_per_sec, font=font)

    w, h = font.getsize(avg_non_food)
    draw.text((1716 - int(w / 2), 1103 - int(h / 2)), avg_non_food, font=font)

    w, h = font.getsize(avg_opaque)
    draw.text((2200 - int(w / 2), 1103 - int(h / 2)), avg_opaque, font=font)

    date = datetime.strptime(input_date, "%d%m%Y").date()
    text = '{} 00:00 -23:59'.format(date.strftime('%B %d, %Y'))
    font = ImageFont.truetype("calibrili.ttf", 70)
    w, h = font.getsize(text)
    draw.text((1250 - int(w / 2), 550 - int(h / 2)), text, font=font, fill=(0, 0, 0))

    text = bottom_note

    if text is not None:
        font = ImageFont.truetype("calibrili.ttf", 50)
        w, h = font.getsize(text)
        draw.text((2450 - w, 3450 - h), text, font=font, fill=(0, 0, 0))

    text = info

    if text is not None:
        font = ImageFont.truetype("calibrib.ttf", 65)
        w, h = font.getsize(text)
        draw.text((2450 - w, 3350 - h), text, font=font, fill=(0, 0, 0))

    image.save(final_image_path)
    pdf = image.convert('RGB')
    pdf.save(final_pdf_path)
